// server.go
package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"text/template"
	"time"

	"github.comc/gorilla/mux"
)

// --- Constantes et Configuration ---
const (
	indexFilePath = "index.idx"
	chunkSize     = 8 * 1024 * 1024 // 8 MB
)

// --- Structures de Données ---

// IndexEntry décrit un unique chunk de fichier.
type IndexEntry struct {
	ChunkID  uint64 // ID unique pour ce chunk
	DiskName string // Nom du disque qui le stocke
	Offset   uint64 // Offset dans le volume.dat du disque
	Size     uint32 // Taille réelle du chunk en bytes
	ChunkIdx int    // Position du chunk dans le fichier (0, 1, 2...)
	Status   byte   // 1: OK, 2: Indisponible, etc.
}

// FileMetadata contient la liste des chunks pour un fichier.
type FileMetadata struct {
	FileName   string
	TotalSize  int64
	UploadDate time.Time
	Chunks     []*IndexEntry
}

// Disk représente un nœud de stockage. Les tailles sont en bytes.
type Disk struct {
	Name       string    `json:"name"`
	Address    string    `json:"address"`
	TotalSpace uint64    `json:"totalSpace"`
	FreeSpace  uint64    `json:"freeSpace"`
	LastSeen   time.Time `json:"-"`
}

// FreeSpaceGB est une méthode d'aide pour convertir les bytes en GB pour l'affichage.
func (d *Disk) FreeSpaceGB() float64 {
	return float64(d.FreeSpace) / (1024 * 1024 * 1024)
}

// GlobalState contient l'état complet du serveur.
type GlobalState struct {
	sync.RWMutex
	FileIndex       map[string]*FileMetadata // Clé: nom du fichier
	RegisteredDisks map[string]*Disk       // Clé: nom du disque
	nextDiskIdx     int
}

var state = GlobalState{
	FileIndex:       make(map[string]*FileMetadata),
	RegisteredDisks: make(map[string]*Disk),
}
var webTemplate *template.Template

// --- Fonctions Principales ---

func main() {
	rand.Seed(time.Now().UnixNano())
	loadIndex()
	go cleanupInactiveDisks()

	var err error
	webTemplate, err = template.New("webui").Parse(htmlTemplate)
	if err != nil {
		log.Fatalf("Impossible de parser le template HTML: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/api/disk/register", registerDiskHandler).Methods("POST")
	r.HandleFunc("/api/files/upload", uploadFileHandler).Methods("POST")
	r.HandleFunc("/api/files/download/{filename}", downloadFileHandler).Methods("GET")
	r.HandleFunc("/", webUIHandler).Methods("GET")

	log.Println("Serveur de stockage démarré sur http://localhost:8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Le serveur n'a pas pu démarrer: %v", err)
	}
}

// --- Gestion de l'Index (Persistance) ---

func loadIndex() {
	state.Lock()
	defer state.Unlock()

	file, err := os.Open(indexFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Fichier d'index '%s' non trouvé. Un nouveau sera créé.", indexFilePath)
			return
		}
		log.Fatalf("Erreur à l'ouverture du fichier d'index: %v", err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&state.FileIndex); err != nil {
		log.Printf("Erreur au décodage de l'index: %v. L'index sera réinitialisé.", err)
		state.FileIndex = make(map[string]*FileMetadata)
	} else {
		log.Printf("Index chargé. %d fichiers indexés.", len(state.FileIndex))
	}
}

func saveIndex() {
	state.RLock()
	defer state.RUnlock()

	file, err := os.Create(indexFilePath)
	if err != nil {
		log.Printf("ERREUR: Impossible de sauvegarder l'index: %v", err)
		return
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(state.FileIndex); err != nil {
		log.Printf("ERREUR: Impossible d'encoder l'index avec gob: %v", err)
	}
}

// --- Logique Métier ---

func selectDisk() *Disk {
	state.Lock()
	defer state.Unlock()

	if len(state.RegisteredDisks) == 0 {
		return nil
	}

	var disks []*Disk
	for _, d := range state.RegisteredDisks {
		disks = append(disks, d)
	}
	sort.Slice(disks, func(i, j int) bool { return disks[i].Name < disks[j].Name })

	if state.nextDiskIdx >= len(disks) {
		state.nextDiskIdx = 0
	}
	selected := disks[state.nextDiskIdx]
	state.nextDiskIdx++

	return selected
}

func cleanupInactiveDisks() {
	for {
		time.Sleep(1 * time.Minute)
		state.Lock()
		for name, disk := range state.RegisteredDisks {
			if time.Since(disk.LastSeen) > 90*time.Second {
				log.Printf("Disque '%s' inactif. Suppression de la liste.", name)
				delete(state.RegisteredDisks, name)
			}
		}
		state.Unlock()
	}
}

// --- Handlers HTTP ---

func registerDiskHandler(w http.ResponseWriter, r *http.Request) {
	var disk Disk
	if err := json.NewDecoder(r.Body).Decode(&disk); err != nil {
		http.Error(w, "JSON invalide: "+err.Error(), http.StatusBadRequest)
		return
	}

	state.Lock()
	disk.LastSeen = time.Now()
	state.RegisteredDisks[disk.Name] = &disk
	state.Unlock()

	log.Printf("Disque enregistré/mis à jour: %s à l'adresse %s", disk.Name, disk.Address)
	w.WriteHeader(http.StatusOK)
}

func uploadFileHandler(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Erreur de lecture du formulaire multipart: "+err.Error(), http.StatusInternalServerError)
		return
	}

	part, err := mr.NextPart()
	if err != nil {
		http.Error(w, "Aucun fichier trouvé dans la requête: "+err.Error(), http.StatusBadRequest)
		return
	}
	fileName := part.FileName()
	if fileName == "" {
		http.Error(w, "Nom de fichier vide.", http.StatusBadRequest)
		return
	}
	log.Printf("Début de l'upload pour: %s", fileName)

	state.RLock()
	_, exists := state.FileIndex[fileName]
	state.RUnlock()
	if exists {
		http.Error(w, "Un fichier avec ce nom existe déjà.", http.StatusConflict)
		return
	}

	var chunks []*IndexEntry
	var totalSize int64
	chunkIdx := 0
	for {
		buffer := make([]byte, chunkSize)
		bytesRead, readErr := io.ReadFull(part, buffer)

		// Gérer la fin de fichier
		if readErr == io.EOF {
			break
		}
		if readErr == io.ErrUnexpectedEOF {
			buffer = buffer[:bytesRead]
		} else if readErr != nil {
			http.Error(w, "Erreur de lecture du chunk: "+readErr.Error(), http.StatusInternalServerError)
			return
		}

		targetDisk := selectDisk()
		if targetDisk == nil {
			http.Error(w, "Aucun disque de stockage disponible.", http.StatusServiceUnavailable)
			return
		}

		diskURL := fmt.Sprintf("http://%s/write_chunk", targetDisk.Address)
		req, _ := http.NewRequest("POST", diskURL, bytes.NewReader(buffer))
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "Erreur interne du serveur (disque injoignable)", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			http.Error(w, "Erreur interne du serveur (le disque a refusé l'écriture)", http.StatusInternalServerError)
			return
		}

		var writeResp struct {
			Offset uint64 `json:"offset"`
			Size   uint32 `json:"size"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&writeResp); err != nil {
			http.Error(w, "Erreur interne du serveur (réponse du disque invalide)", http.StatusInternalServerError)
			return
		}

		entry := &IndexEntry{
			ChunkID:  uint64(time.Now().UnixNano()) + uint64(rand.Int()),
			DiskName: targetDisk.Name,
			Offset:   writeResp.Offset,
			Size:     writeResp.Size,
			ChunkIdx: chunkIdx,
			Status:   1, // OK
		}
		chunks = append(chunks, entry)
		totalSize += int64(writeResp.Size)
		chunkIdx++

		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	state.Lock()
	state.FileIndex[fileName] = &FileMetadata{
		FileName:   fileName,
		TotalSize:  totalSize,
		UploadDate: time.Now(),
		Chunks:     chunks,
	}
	state.Unlock()

	saveIndex()
	log.Printf("Fichier %s (taille: %d, chunks: %d) uploadé avec succès.", fileName, totalSize, len(chunks))
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func downloadFileHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	state.RLock()
	meta, ok := state.FileIndex[filename]
	if !ok {
		state.RUnlock()
		http.NotFound(w, r)
		return
	}
	chunks := make([]*IndexEntry, len(meta.Chunks))
	copy(chunks, meta.Chunks)
	state.RUnlock()

	w.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.TotalSize))

	for _, chunk := range chunks {
		state.RLock()
		disk, diskOK := state.RegisteredDisks[chunk.DiskName]
		state.RUnlock()

		if !diskOK {
			http.Error(w, "Une partie du fichier est indisponible (disque manquant)", http.StatusServiceUnavailable)
			return
		}

		diskURL := fmt.Sprintf("http://%s/read_chunk?offset=%d&size=%d", disk.Address, chunk.Offset, chunk.Size)
		resp, err := http.Get(diskURL)
		if err != nil {
			http.Error(w, "Erreur de lecture d'une partie du fichier", http.StatusInternalServerError)
			return
		}
		
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			http.Error(w, "Erreur de lecture d'une partie du fichier (le disque a échoué)", http.StatusInternalServerError)
			return
		}

		_, err = io.Copy(w, resp.Body)
		resp.Body.Close()
		if err != nil {
			return
		}
	}
}

func webUIHandler(w http.ResponseWriter, r *http.Request) {
	state.RLock()
	defer state.RUnlock()

	disks := make([]*Disk, 0, len(state.RegisteredDisks))
	for _, d := range state.RegisteredDisks {
		disks = append(disks, d)
	}

	files := make([]*FileMetadata, 0, len(state.FileIndex))
	for _, f := range state.FileIndex {
		files = append(files, f)
	}

	sort.Slice(disks, func(i, j int) bool { return disks[i].Name < disks[j].Name })
	sort.Slice(files, func(i, j int) bool { return files[i].UploadDate.After(files[j].UploadDate) })

	data := struct {
		Disks []*Disk
		Files []*FileMetadata
	}{
		Disks: disks,
		Files: files,
	}

	err := webTemplate.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

const htmlTemplate = `
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8"><title>Stockage Distribué</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f7f6; color: #333; margin: 2em; }
        .container { max-width: 1200px; margin: auto; background: white; padding: 2em; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); }
        h1, h2 { color: #2c3e50; border-bottom: 2px solid #e0e0e0; padding-bottom: 0.5em;}
        .grid { display: grid; grid-template-columns: 1fr 2fr; gap: 2em; }
        table { width: 100%; border-collapse: collapse; margin-top: 1em; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        .upload-form { background: #f9f9f9; padding: 1.5em; border-radius: 5px; border: 1px solid #ddd; }
        .btn { background-color: #3498db; color: white; padding: 10px 15px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; }
        .btn-download { background-color: #27ae60; }
    </style>
</head>
<body>
    <div class="container">
        <h1>💿 Panneau de Contrôle du Stockage</h1>
        <div class="grid">
            <div>
                <h2>Disques Actifs ({{len .Disks}})</h2>
                <table>
                    <thead><tr><th>Nom</th><th>Adresse</th><th>Espace Libre</th></tr></thead>
                    <tbody>
                    {{range .Disks}}
                        <tr><td>{{.Name}}</td><td>{{.Address}}</td><td>{{.FreeSpaceGB | printf "%.2f"}} GB</td></tr>
                    {{else}}
                        <tr><td colspan="3">Aucun disque connecté.</td></tr>
                    {{end}}
                    </tbody>
                </table>
            </div>
            <div>
                <h2>Fichiers Stockés ({{len .Files}})</h2>
                <div class="upload-form">
                    <h3>Ajouter un nouveau fichier</h3>
                    <form action="/api/files/upload" method="post" enctype="multipart/form-data">
                        <input type="file" name="file" required>
                        <button type="submit" class="btn">Envoyer</button>
                    </form>
                </div>
                <table>
                    <thead><tr><th>Nom</th><th>Taille (MB)</th><th>Chunks</th><th>Date d'ajout</th><th>Action</th></tr></thead>
                    <tbody>
                    {{range .Files}}
                        <tr>
                            <td>{{.FileName}}</td>
                            <td>{{printf "%.2f" (float64 .TotalSize / 1048576.0)}}</td>
                            <td>{{len .Chunks}}</td>
                            <td>{{.UploadDate.Format "02/01/2006 15:04"}}</td>
                            <td><a href="/api/files/download/{{.FileName}}" class="btn btn-download">Télécharger</a></td>
                        </tr>
                    {{else}}
                        <tr><td colspan="5">Aucun fichier stocké.</td></tr>
                    {{end}}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</body>
</html>`
