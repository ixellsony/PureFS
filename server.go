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

	"github.com/gorilla/mux"
)

// --- Constantes et Configuration ---
const (
	indexFilePath = "index.idx"
	chunkSize     = 8 * 1024 * 1024 // 8 MB
)

// --- Structures de Données ---

type IndexEntry struct {
	ChunkID  uint64
	DiskName string
	Offset   uint64
	Size     uint32
	ChunkIdx int
	Status   byte
}

type FileMetadata struct {
	FileName   string
	TotalSize  int64
	UploadDate time.Time
	Chunks     []*IndexEntry
}

func (fm *FileMetadata) TotalSizeMB() float64 {
	return float64(fm.TotalSize) / (1024 * 1024)
}

type Disk struct {
	Name       string    `json:"name"`
	Address    string    `json:"address"`
	TotalSpace uint64    `json:"totalSpace"`
	FreeSpace  uint64    `json:"freeSpace"`
	LastSeen   time.Time `json:"-"`
	Status     string    `json:"status"` // "En ligne" ou "Hors ligne"
}

func (d *Disk) FreeSpaceGB() float64 {
	return float64(d.FreeSpace) / (1024 * 1024 * 1024)
}

type GlobalState struct {
	sync.RWMutex
	FileIndex       map[string]*FileMetadata
	RegisteredDisks map[string]*Disk
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

// --- Logique Métier (Index, Sélection de disque, etc.) ---

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

func selectDisk() *Disk {
	state.Lock()
	defer state.Unlock()

	var onlineDisks []*Disk
	for _, d := range state.RegisteredDisks {
		if d.Status == "En ligne" {
			onlineDisks = append(onlineDisks, d)
		}
	}

	if len(onlineDisks) == 0 {
		return nil
	}

	sort.Slice(onlineDisks, func(i, j int) bool { return onlineDisks[i].Name < onlineDisks[j].Name })
	if state.nextDiskIdx >= len(onlineDisks) {
		state.nextDiskIdx = 0
	}
	selected := onlineDisks[state.nextDiskIdx]
	state.nextDiskIdx++
	return selected
}

func cleanupInactiveDisks() {
	ticker := time.NewTicker(30 * time.Second) // Vérification toutes les 30 secondes
	defer ticker.Stop()

	for range ticker.C {
		state.Lock()
		for _, disk := range state.RegisteredDisks {
			if time.Since(disk.LastSeen) > 45*time.Second && disk.Status == "En ligne" {
				log.Printf("Disque '%s' inactif. Marquage comme Hors ligne.", disk.Name)
				disk.Status = "Hors ligne"
			}
		}
		state.Unlock()
	}
}

// --- Handlers HTTP ---

// MODIFICATION MAJEURE: La logique d'enregistrement gère les conflits de nom.
func registerDiskHandler(w http.ResponseWriter, r *http.Request) {
	var diskData Disk
	if err := json.NewDecoder(r.Body).Decode(&diskData); err != nil {
		http.Error(w, "JSON invalide: "+err.Error(), http.StatusBadRequest)
		return
	}
	if diskData.Name == "" {
		http.Error(w, "Le nom du disque ne peut pas être vide.", http.StatusBadRequest)
		return
	}

	state.Lock()
	defer state.Unlock()

	existingDisk, exists := state.RegisteredDisks[diskData.Name]

	// Cas 1: Le disque est déjà connu.
	if exists {
		// Cas 1a: CONFLIT. Un disque avec ce nom est déjà considéré comme "En ligne".
		// On refuse la connexion pour éviter d'avoir deux volumes actifs avec le même nom.
		// Cela se produit lors de l'enregistrement initial d'un volume dont le nom est déjà pris.
		if existingDisk.Status == "En ligne" {
			// On considère que c'est un heartbeat légitime s'il vient de la même adresse.
			// (Simple protection, pourrait être améliorée)
			if existingDisk.Address == diskData.Address {
				existingDisk.LastSeen = time.Now()
				existingDisk.FreeSpace = diskData.FreeSpace
				log.Printf("Heartbeat de '%s' (même adresse).", existingDisk.Name)
				w.WriteHeader(http.StatusOK)
				return
			}

			log.Printf("Conflit de nom de volume : Tentative d'enregistrement pour '%s' depuis %s alors qu'il est déjà en ligne à l'adresse %s.", diskData.Name, diskData.Address, existingDisk.Address)
			http.Error(w, fmt.Sprintf("Impossible d'enregistrer le volume : un volume nommé '%s' est déjà connecté au serveur.", diskData.Name), http.StatusConflict)
			return
		}

		// Cas 1b: RECONNEXION. Le disque était "Hors ligne" et revient. On met à jour ses informations.
		log.Printf("Le disque '%s' est de retour en ligne.", diskData.Name)
		existingDisk.Address = diskData.Address
		existingDisk.TotalSpace = diskData.TotalSpace
		existingDisk.FreeSpace = diskData.FreeSpace
		existingDisk.LastSeen = time.Now()
		existingDisk.Status = "En ligne"

	} else {
		// Cas 2: NOUVEAU DISQUE. Il n'a jamais été vu auparavant.
		log.Printf("Nouveau disque enregistré : %s", diskData.Name)
		state.RegisteredDisks[diskData.Name] = &Disk{
			Name:       diskData.Name,
			Address:    diskData.Address,
			TotalSpace: diskData.TotalSpace,
			FreeSpace:  diskData.FreeSpace,
			LastSeen:   time.Now(),
			Status:     "En ligne",
		}
	}

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
			state.Lock()
			if d, ok := state.RegisteredDisks[targetDisk.Name]; ok {
				d.Status = "Hors ligne"
			}
			state.Unlock()
			log.Printf("ERREUR: Le disque '%s' est injoignable. Marquage comme Hors ligne.", targetDisk.Name)
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

		if !diskOK || disk.Status == "Hors ligne" {
			log.Printf("Échec du téléchargement: le disque '%s' pour le fichier '%s' est hors ligne.", chunk.DiskName, filename)
			http.Error(w, fmt.Sprintf("Une partie du fichier est indisponible (disque '%s' est hors ligne)", chunk.DiskName), http.StatusServiceUnavailable)
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
        .status-online { color: #27ae60; font-weight: bold; }
        .status-offline { color: #c0392b; font-weight: bold; }
        .disk-offline td { color: #95a5a6; text-decoration: line-through; }
    </style>
</head>
<body>
    <div class="container">
        <h1>💿 Panneau de Contrôle du Stockage</h1>
        <div class="grid">
            <div>
                <h2>Disques ({{len .Disks}})</h2>
                <table>
                    <thead><tr><th>Nom</th><th>Adresse</th><th>Espace Libre</th><th>Statut</th></tr></thead>
                    <tbody>
                    {{range .Disks}}
                        <tr class="{{if eq .Status "Hors ligne"}}disk-offline{{end}}">
                            <td>{{.Name}}</td>
                            <td>{{.Address}}</td>
                            <td>{{.FreeSpaceGB | printf "%.2f"}} GB</td>
                            <td>
                                {{if eq .Status "En ligne"}}
                                    <span class="status-online">● En ligne</span>
                                {{else}}
                                    <span class="status-offline">● Hors ligne</span>
                                {{end}}
                            </td>
                        </tr>
                    {{else}}
                        <tr><td colspan="4">Aucun disque enregistré.</td></tr>
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
                            <td>{{.TotalSizeMB | printf "%.2f"}}</td>
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
