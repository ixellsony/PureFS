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
	indexFilePath    = "index.idx"
	chunkSize        = 8 * 1024 * 1024 // 8 MB
	requiredReplicas = 2
)

// --- Structures de Données ---

type ChunkCopy struct {
	VolumeName string // Nom du volume, ex: "volume1"
	Offset     uint64
	Size       uint32
}

type ChunkMetadata struct {
	ChunkIdx int
	Copies   []*ChunkCopy
}

type FileMetadata struct {
	FileName   string
	TotalSize  int64
	UploadDate time.Time
	Chunks     []*ChunkMetadata
	// Champs volatiles pour l'UI
	Status         string `json:"-"` // "Protégé", "Dégradé", "Indisponible"
	HealthyCopies  int    `json:"-"`
	RequiredCopies int    `json:"-"`
}

func (fm *FileMetadata) TotalSizeMB() float64 {
	return float64(fm.TotalSize) / (1024 * 1024)
}

type Volume struct {
	Name       string    `json:"name"`
	DiskID     string    `json:"diskId"` // NOUVEAU: ID du disque physique
	Address    string    `json:"address"`
	TotalSpace uint64    `json:"totalSpace"`
	FreeSpace  uint64    `json:"freeSpace"`
	LastSeen   time.Time `json:"-"`
	Status     string    `json:"status"` // "En ligne" ou "Hors ligne"
	Type       string    `json:"type,omitempty"`
}

func (v *Volume) FreeSpaceGB() float64 {
	return float64(v.FreeSpace) / (1024 * 1024 * 1024)
}

type GlobalState struct {
	sync.RWMutex
	FileIndex         map[string]*FileMetadata
	RegisteredVolumes map[string]*Volume
}

var state = GlobalState{
	FileIndex:         make(map[string]*FileMetadata),
	RegisteredVolumes: make(map[string]*Volume),
}
var webTemplate *template.Template

// --- Fonctions Principales ---
func main() {
	rand.Seed(time.Now().UnixNano())
	loadIndex()
	go cleanupInactiveVolumes()

	var err error
	webTemplate, err = template.New("webui").Parse(htmlTemplate)
	if err != nil {
		log.Fatalf("Impossible de parser le template HTML: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/api/disk/register", registerVolumeHandler).Methods("POST")
	r.HandleFunc("/api/files/upload", uploadFileHandler).Methods("POST")
	r.HandleFunc("/api/files/download/{filename}", downloadFileHandler).Methods("GET")
	r.HandleFunc("/api/repair", repairHandler).Methods("POST") // NOUVELLE ROUTE
	r.HandleFunc("/", webUIHandler).Methods("GET")

	log.Println("Serveur de stockage démarré sur http://localhost:8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Le serveur n'a pas pu démarrer: %v", err)
	}
}

// --- Logique Métier ---
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

// NOUVELLE LOGIQUE DE SÉLECTION: Sélectionne N volumes sur des disques physiques différents.
func selectVolumesForReplication(count int, excludeDisks []string) ([]*Volume, error) {
	state.RLock()
	defer state.RUnlock()

	var onlineVolumes []*Volume
	for _, v := range state.RegisteredVolumes {
		if v.Status == "En ligne" {
			onlineVolumes = append(onlineVolumes, v)
		}
	}

	// Grouper les volumes par leur disque physique parent
	volumesByDiskID := make(map[string][]*Volume)
	for _, v := range onlineVolumes {
		isExcluded := false
		for _, excluded := range excludeDisks {
			if v.DiskID == excluded {
				isExcluded = true
				break
			}
		}
		if !isExcluded {
			volumesByDiskID[v.DiskID] = append(volumesByDiskID[v.DiskID], v)
		}
	}

	if len(volumesByDiskID) < count {
		return nil, fmt.Errorf("pas assez de disques physiques uniques disponibles (requis: %d, dispo: %d)", count, len(volumesByDiskID))
	}

	// Sélectionner les disques physiques à utiliser
	var availableDiskIDs []string
	for diskID := range volumesByDiskID {
		availableDiskIDs = append(availableDiskIDs, diskID)
	}
	rand.Shuffle(len(availableDiskIDs), func(i, j int) {
		availableDiskIDs[i], availableDiskIDs[j] = availableDiskIDs[j], availableDiskIDs[i]
	})

	selectedDisks := availableDiskIDs[:count]
	var result []*Volume
	for _, diskID := range selectedDisks {
		// Choisir un volume au hasard sur ce disque
		volumesOnThisDisk := volumesByDiskID[diskID]
		selectedVolume := volumesOnThisDisk[rand.Intn(len(volumesOnThisDisk))]
		result = append(result, selectedVolume)
	}

	return result, nil
}

func cleanupInactiveVolumes() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		state.Lock()
		for _, volume := range state.RegisteredVolumes {
			if time.Since(volume.LastSeen) > 45*time.Second && volume.Status == "En ligne" {
				log.Printf("Volume '%s' inactif. Marquage comme Hors ligne.", volume.Name)
				volume.Status = "Hors ligne"
			}
		}
		state.Unlock()
	}
}

// --- Handlers HTTP ---

func registerVolumeHandler(w http.ResponseWriter, r *http.Request) {
	var volData Volume
	if err := json.NewDecoder(r.Body).Decode(&volData); err != nil {
		http.Error(w, "JSON invalide: "+err.Error(), http.StatusBadRequest)
		return
	}
	if volData.Name == "" || volData.DiskID == "" {
		http.Error(w, "Le nom du volume et l'ID du disque sont requis.", http.StatusBadRequest)
		return
	}

	state.Lock()
	defer state.Unlock()

	existingVol, exists := state.RegisteredVolumes[volData.Name]

	updateVolume := func(v *Volume) {
		v.Address = volData.Address
		v.DiskID = volData.DiskID
		v.TotalSpace = volData.TotalSpace
		v.FreeSpace = volData.FreeSpace
		v.LastSeen = time.Now()
		v.Status = "En ligne"
	}

	if volData.Type == "initial" {
		if exists && existingVol.Status == "En ligne" {
			log.Printf("Conflit de nom de volume : Tentative d'enregistrement initial pour '%s' alors qu'il est déjà en ligne.", volData.Name)
			http.Error(w, fmt.Sprintf("Impossible d'enregistrer le volume : un volume nommé '%s' est déjà connecté.", volData.Name), http.StatusConflict)
			return
		}
		if !exists {
			log.Printf("Nouveau volume enregistré : %s sur disque %s", volData.Name, volData.DiskID)
			existingVol = &Volume{Name: volData.Name}
			state.RegisteredVolumes[volData.Name] = existingVol
		} else {
			log.Printf("Le volume '%s' (précédemment hors ligne) est de retour.", volData.Name)
		}
		updateVolume(existingVol)
		w.WriteHeader(http.StatusOK)
		return
	}

	if volData.Type == "heartbeat" {
		if !exists {
			log.Printf("Avertissement : Heartbeat reçu pour un volume inconnu '%s'. Enregistrement...", volData.Name)
			existingVol = &Volume{Name: volData.Name}
			state.RegisteredVolumes[volData.Name] = existingVol
		}
		updateVolume(existingVol)
		w.WriteHeader(http.StatusOK)
		return
	}

	http.Error(w, "Type de requête ('initial' ou 'heartbeat') manquant.", http.StatusBadRequest)
}

func uploadFileHandler(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "Erreur de lecture du formulaire multipart: "+err.Error(), http.StatusInternalServerError)
		return
	}
	part, err := mr.NextPart()
	if err != nil {
		http.Error(w, "Aucun fichier trouvé: "+err.Error(), http.StatusBadRequest)
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

	var fileChunks []*ChunkMetadata
	var totalSize int64
	chunkIdx := 0
	for {
		buffer := make([]byte, chunkSize)
		bytesRead, readErr := io.ReadFull(part, buffer)

		isLastChunk := false
		if readErr == io.EOF {
			break // Fichier vide ou terminé parfaitement à la fin du chunk précédent
		}
		if readErr == io.ErrUnexpectedEOF {
			buffer = buffer[:bytesRead]
			isLastChunk = true
		} else if readErr != nil {
			http.Error(w, "Erreur de lecture du chunk: "+readErr.Error(), http.StatusInternalServerError)
			return
		}

		targetVolumes, err := selectVolumesForReplication(requiredReplicas, nil)
		if err != nil {
			log.Printf("ERREUR UPLOAD: Impossible de trouver des volumes pour la réplication: %v", err)
			http.Error(w, "Erreur interne: "+err.Error(), http.StatusServiceUnavailable)
			return
		}

		log.Printf("Chunk %d: écriture sur %s (disque %s) et %s (disque %s)",
			chunkIdx, targetVolumes[0].Name, targetVolumes[0].DiskID, targetVolumes[1].Name, targetVolumes[1].DiskID)

		var wg sync.WaitGroup
		results := make(chan *ChunkCopy, requiredReplicas)
		errs := make(chan error, requiredReplicas)

		for _, vol := range targetVolumes {
			wg.Add(1)
			go func(v *Volume) {
				defer wg.Done()
				diskURL := fmt.Sprintf("http://%s/write_chunk", v.Address)
				req, _ := http.NewRequest("POST", diskURL, bytes.NewReader(buffer))
				req.Header.Set("Content-Type", "application/octet-stream")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					errs <- fmt.Errorf("volume '%s' injoignable: %w", v.Name, err)
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					errs <- fmt.Errorf("le volume '%s' a refusé l'écriture", v.Name)
					return
				}
				var writeResp struct {
					Offset uint64 `json:"offset"`
					Size   uint32 `json:"size"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&writeResp); err != nil {
					errs <- fmt.Errorf("réponse invalide du volume '%s'", v.Name)
					return
				}
				results <- &ChunkCopy{VolumeName: v.Name, Offset: writeResp.Offset, Size: writeResp.Size}
			}(vol)
		}
		wg.Wait()
		close(results)
		close(errs)

		var successfulCopies []*ChunkCopy
		for res := range results {
			successfulCopies = append(successfulCopies, res)
		}
		for e := range errs {
			log.Printf("ERREUR UPLOAD: %v", e)
		}

		if len(successfulCopies) < requiredReplicas {
			http.Error(w, "Échec de l'écriture sur un ou plusieurs volumes. L'upload est annulé.", http.StatusInternalServerError)
			return
		}

		fileChunks = append(fileChunks, &ChunkMetadata{ChunkIdx: chunkIdx, Copies: successfulCopies})
		totalSize += int64(successfulCopies[0].Size)
		chunkIdx++

		if isLastChunk {
			break
		}
	}

	state.Lock()
	state.FileIndex[fileName] = &FileMetadata{
		FileName:   fileName,
		TotalSize:  totalSize,
		UploadDate: time.Now(),
		Chunks:     fileChunks,
	}
	state.Unlock()
	saveIndex()
	log.Printf("Fichier %s (taille: %d, chunks: %d) uploadé avec succès.", fileName, totalSize, len(fileChunks))
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
	chunks := make([]*ChunkMetadata, len(meta.Chunks))
	copy(chunks, meta.Chunks)
	state.RUnlock()

	w.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.TotalSize))

	for _, chunk := range chunks {
		var readSuccessful bool
		var lastError error
		for _, copyInfo := range chunk.Copies {
			state.RLock()
			volume, volOK := state.RegisteredVolumes[copyInfo.VolumeName]
			state.RUnlock()

			if !volOK || volume.Status != "En ligne" {
				lastError = fmt.Errorf("volume '%s' hors ligne", copyInfo.VolumeName)
				continue
			}

			diskURL := fmt.Sprintf("http://%s/read_chunk?offset=%d&size=%d", volume.Address, copyInfo.Offset, copyInfo.Size)
			resp, err := http.Get(diskURL)
			if err != nil {
				lastError = fmt.Errorf("erreur de connexion à '%s'", volume.Name)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				lastError = fmt.Errorf("échec de lecture sur '%s'", volume.Name)
				continue
			}

			_, err = io.Copy(w, resp.Body)
			resp.Body.Close()
			if err != nil {
				log.Printf("Erreur d'envoi du chunk au client: %v", err)
				return
			}
			readSuccessful = true
			break
		}

		if !readSuccessful {
			log.Printf("Échec du téléchargement pour '%s': aucune copie du chunk %d n'est disponible. Dernière erreur: %v", filename, chunk.ChunkIdx, lastError)
			http.Error(w, fmt.Sprintf("Fichier corrompu ou indisponible, impossible de lire le chunk %d", chunk.ChunkIdx), http.StatusServiceUnavailable)
			return
		}
	}
}

// CORRIGÉ: Handler pour lancer la réparation avec une meilleure gestion des verrous
func repairHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Lancement de la procédure de réparation en arrière-plan...")
	go runRepairProcess()
	// Redirige immédiatement l'utilisateur, la réparation se fait en tâche de fond.
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// CORRIGÉ: La logique de réparation est extraite dans sa propre fonction
func runRepairProcess() {
	// Étape 1: Identifier les chunks à réparer avec un verrou en lecture
	type repairJob struct {
		filename     string
		chunkIdx     int
		sourceCopy   *ChunkCopy
		sourceVolume *Volume
	}
	var jobs []repairJob

	state.RLock()
	for filename, meta := range state.FileIndex {
		for _, chunk := range meta.Chunks {
			var onlineCopies []*ChunkCopy
			var excludeDiskIDs []string
			for _, copyInfo := range chunk.Copies {
				if vol, ok := state.RegisteredVolumes[copyInfo.VolumeName]; ok && vol.Status == "En ligne" {
					onlineCopies = append(onlineCopies, copyInfo)
					excludeDiskIDs = append(excludeDiskIDs, vol.DiskID)
				}
			}

			// Si le chunk est dégradé (réparable) et qu'il y a un disque de destination possible
			if len(onlineCopies) > 0 && len(onlineCopies) < requiredReplicas {
				sourceCopy := onlineCopies[0]
				sourceVolume := state.RegisteredVolumes[sourceCopy.VolumeName]
				jobs = append(jobs, repairJob{filename, chunk.ChunkIdx, sourceCopy, sourceVolume})
			}
		}
	}
	state.RUnlock()

	if len(jobs) == 0 {
		log.Println("Procédure de réparation terminée. Aucun chunk dégradé et réparable n'a été trouvé.")
		return
	}

	log.Printf("Début de la réparation pour %d chunks...", len(jobs))
	repairedChunks := 0

	// Étape 2: Exécuter les jobs SANS verrou global
	for _, job := range jobs {
		// Sélectionner une destination. On le fait ici pour avoir l'état le plus récent des disques.
		excludeDiskIDs := []string{}
		state.RLock()
		for _, c := range state.FileIndex[job.filename].Chunks[job.chunkIdx].Copies {
			if v, ok := state.RegisteredVolumes[c.VolumeName]; ok {
				excludeDiskIDs = append(excludeDiskIDs, v.DiskID)
			}
		}
		targetVolumes, err := selectVolumesForReplication(1, excludeDiskIDs)
		state.RUnlock()
		
		if err != nil {
			log.Printf("ERREUR REPARATION: Impossible de trouver un volume de destination pour chunk %d de '%s': %v", job.chunkIdx, job.filename, err)
			continue
		}
		targetVolume := targetVolumes[0]

		// Lire le chunk source
		readURL := fmt.Sprintf("http://%s/read_chunk?offset=%d&size=%d", job.sourceVolume.Address, job.sourceCopy.Offset, job.sourceCopy.Size)
		resp, err := http.Get(readURL)
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Printf("ERREUR REPARATION: Impossible de lire le chunk source depuis '%s': %v", job.sourceVolume.Name, err)
			if resp != nil { resp.Body.Close() }
			continue
		}
		chunkData, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("ERREUR REPARATION: Impossible de lire le corps de la réponse de '%s': %v", job.sourceVolume.Name, err)
			continue
		}

		// Écrire le chunk sur la destination
		writeURL := fmt.Sprintf("http://%s/write_chunk", targetVolume.Address)
		req, _ := http.NewRequest("POST", writeURL, bytes.NewReader(chunkData))
		req.Header.Set("Content-Type", "application/octet-stream")
		writeResp, err := http.DefaultClient.Do(req)
		if err != nil || writeResp.StatusCode != http.StatusOK {
			log.Printf("ERREUR REPARATION: Impossible d'écrire le chunk sur la destination '%s': %v", targetVolume.Name, err)
			if writeResp != nil { writeResp.Body.Close() }
			continue
		}

		var writeRespBody struct {
			Offset uint64 `json:"offset"`
			Size   uint32 `json:"size"`
		}
		if err := json.NewDecoder(writeResp.Body).Decode(&writeRespBody); err != nil {
			writeResp.Body.Close()
			log.Printf("ERREUR REPARATION: Réponse JSON invalide de '%s'", targetVolume.Name)
			continue
		}
		writeResp.Body.Close()

		// Étape 3: Mettre à jour les métadonnées avec un verrou en écriture court
		state.Lock()
		newCopy := &ChunkCopy{
			VolumeName: targetVolume.Name,
			Offset:     writeRespBody.Offset,
			Size:       writeRespBody.Size,
		}
		// S'assurer que le fichier et le chunk existent toujours (au cas où il a été supprimé)
		if meta, ok := state.FileIndex[job.filename]; ok && job.chunkIdx < len(meta.Chunks) {
			meta.Chunks[job.chunkIdx].Copies = append(meta.Chunks[job.chunkIdx].Copies, newCopy)
			repairedChunks++
			log.Printf("Succès: Chunk %d de '%s' recopié de '%s' vers '%s' (Disque: %s)",
				job.chunkIdx, job.filename, job.sourceVolume.Name, targetVolume.Name, targetVolume.DiskID)
		}
		state.Unlock()
	}

	if repairedChunks > 0 {
		saveIndex()
		log.Printf("Procédure de réparation terminée. %d chunks ont été restaurés.", repairedChunks)
	} else {
		log.Println("Procédure de réparation terminée. Aucun chunk n'a pu être réparé.")
	}
}

func webUIHandler(w http.ResponseWriter, r *http.Request) {
	state.RLock()
	defer state.RUnlock()

	// Préparation des données pour le template
	volumes := make([]*Volume, 0, len(state.RegisteredVolumes))
	onlinePhysicalDisks := make(map[string]bool)
	for _, v := range state.RegisteredVolumes {
		volumes = append(volumes, v)
		if v.Status == "En ligne" {
			onlinePhysicalDisks[v.DiskID] = true
		}
	}

	files := make([]*FileMetadata, 0, len(state.FileIndex))
	filesInDegradedState := false
	isRepairPossible := false

	for _, f := range state.FileIndex {
		// Calculer l'état de santé du fichier
		isUnavailable := false
		isDegraded := false
		for _, chunk := range f.Chunks {
			onlineCopies := 0
			onlineCopyDisks := make(map[string]bool)
			for _, copyInfo := range chunk.Copies {
				if vol, ok := state.RegisteredVolumes[copyInfo.VolumeName]; ok && vol.Status == "En ligne" {
					onlineCopies++
					onlineCopyDisks[vol.DiskID] = true
				}
			}
			if onlineCopies == 0 {
				isUnavailable = true
				break
			}
			if onlineCopies < requiredReplicas {
				isDegraded = true
				// CORRIGÉ: La réparation est possible s'il y a un disque en ligne qui N'A PAS déjà une copie de ce chunk
				if len(onlinePhysicalDisks) > len(onlineCopyDisks) {
					isRepairPossible = true
				}
			}
		}

		if isUnavailable {
			f.Status = "Indisponible"
		} else if isDegraded {
			f.Status = "Dégradé"
			filesInDegradedState = true
		} else {
			f.Status = "Protégé"
		}
		files = append(files, f)
	}

	notEnoughDisksForRedundancy := len(onlinePhysicalDisks) < requiredReplicas

	// Tri pour un affichage stable
	sort.Slice(volumes, func(i, j int) bool {
		if volumes[i].DiskID == volumes[j].DiskID {
			return volumes[i].Name < volumes[j].Name
		}
		return volumes[i].DiskID < volumes[j].DiskID
	})
	sort.Slice(files, func(i, j int) bool { return files[i].UploadDate.After(files[j].UploadDate) })

	data := struct {
		Volumes                     []*Volume
		Files                       []*FileMetadata
		FilesInDegradedState        bool
		NotEnoughDisksForRedundancy bool
		RequiredReplicas            int
		CanRepair                   bool // La variable est conservée, sa logique de remplissage est juste améliorée
	}{
		Volumes:                     volumes,
		Files:                       files,
		FilesInDegradedState:        filesInDegradedState,
		NotEnoughDisksForRedundancy: notEnoughDisksForRedundancy,
		RequiredReplicas:            requiredReplicas,
		CanRepair:                   isRepairPossible, // Utilise la nouvelle logique plus fine
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
		.alert { padding: 1em; margin-bottom: 1em; border-radius: 5px; display: flex; justify-content: space-between; align-items: center; }
		.alert-warning { background-color: #fcf8e3; border: 1px solid #faebcc; color: #8a6d3b; }
        .alert-danger { background-color: #f2dede; border: 1px solid #ebccd1; color: #a94442; }
        .grid { display: grid; grid-template-columns: 1fr 2fr; gap: 2em; }
        table { width: 100%; border-collapse: collapse; margin-top: 1em; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        .upload-form { background: #f9f9f9; padding: 1.5em; border-radius: 5px; border: 1px solid #ddd; }
        .btn { background-color: #3498db; color: white; padding: 10px 15px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; font-size: 14px; }
        .btn-download { background-color: #27ae60; }
        .btn-repair { background-color: #e67e22; }
		.btn-disabled { background-color: #bdc3c7; cursor: not-allowed; }
        .status-online { color: #27ae60; font-weight: bold; }
        .status-offline { color: #c0392b; font-weight: bold; }
        .status-protected { color: #27ae60; }
        .status-degraded { color: #f39c12; }
        .status-unavailable { color: #c0392b; font-weight: bold; }
        .volume-offline td { color: #95a5a6; }
    </style>
</head>
<body>
    <div class="container">
        <h1>💿 Panneau de Contrôle du Stockage</h1>
        
        {{if .NotEnoughDisksForRedundancy}}
        <div class="alert alert-danger">
            <strong>Attention !</strong> Pas assez de disques physiques en ligne pour garantir la duplication ({{.RequiredReplicas}} requis). Les nouveaux uploads échoueront.
        </div>
        {{end}}
        {{if .FilesInDegradedState}}
        <div class="alert alert-warning">
			<div>
				<strong>Redondance compromise !</strong> Certains fichiers sont dégradés car un disque est hors ligne.
				<br>Les données sont toujours accessibles mais ne sont plus dupliquées.
			</div>
			<div>
				<form action="/api/repair" method="post" style="margin:0;">
					<button type="submit" class="btn btn-repair" {{if not .CanRepair}}disabled title="Pas assez de disques différents en ligne pour la réparation"{{end}}>
						Effectuer une réparation
					</button>
				</form>
			</div>
        </div>
        {{end}}

        <div class="grid">
            <div>
                <h2>Volumes ({{len .Volumes}})</h2>
                <table>
                    <thead><tr><th>Disque Physique</th><th>Nom Volume</th><th>Espace Libre</th><th>Statut</th></tr></thead>
                    <tbody>
                    {{range .Volumes}}
                        <tr class="{{if eq .Status "Hors ligne"}}volume-offline{{end}}">
                            <td>{{.DiskID}}</td>
                            <td>{{.Name}}</td>
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
                        <tr><td colspan="4">Aucun volume enregistré.</td></tr>
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
                        <button type="submit" class="btn" {{if .NotEnoughDisksForRedundancy}}disabled title="Pas assez de disques pour l'upload"{{end}}>Envoyer</button>
                    </form>
                </div>
                <table>
                    <thead><tr><th>Nom</th><th>Taille</th><th>Chunks</th><th>Date</th><th>Statut de Redondance</th><th>Action</th></tr></thead>
                    <tbody>
                    {{range .Files}}
                        <tr>
                            <td>{{.FileName}}</td>
                            <td>{{.TotalSizeMB | printf "%.2f"}} MB</td>
                            <td>{{len .Chunks}}</td>
                            <td>{{.UploadDate.Format "02/01/2006 15:04"}}</td>
                            <td>
                                {{if eq .Status "Protégé"}} <span class="status-protected">✔ Protégé</span>
                                {{else if eq .Status "Dégradé"}} <span class="status-degraded">⚠ Dégradé</span>
                                {{else}} <span class="status-unavailable">✖ Indisponible</span>
                                {{end}}
                            </td>
                            <td>
                                <a href="/api/files/download/{{.FileName}}" 
                                   class="btn btn-download {{if eq .Status "Indisponible"}}btn-disabled{{end}}"
                                   {{if eq .Status "Indisponible"}}onclick="return false;"{{end}}>
                                   Télécharger
                                </a>
                            </td>
                        </tr>
                    {{else}}
                        <tr><td colspan="6">Aucun fichier stocké.</td></tr>
                    {{end}}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</body>
</html>
`
