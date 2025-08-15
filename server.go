package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
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
	VolumeName string
	Offset     uint64
	Size       uint32
	Deleted    bool `json:"deleted,omitempty"` // Marqué pour suppression
}

type ChunkMetadata struct {
	ChunkIdx int
	Copies   []*ChunkCopy
	Checksum string
}

type FileMetadata struct {
	FileName   string
	TotalSize  int64
	UploadDate time.Time
	Chunks     []*ChunkMetadata
	Status     string `json:"-"`
	Deleted    bool   `json:"deleted,omitempty"` // Marqué pour suppression
}

func (fm *FileMetadata) TotalSizeMB() float64 {
	return float64(fm.TotalSize) / (1024 * 1024)
}

type Volume struct {
	Name       string
	DiskID     string
	Address    string
	TotalSpace uint64
	FreeSpace  uint64
	LastSeen   time.Time `json:"-"`
	Status     string
	Type       string `json:"type,omitempty"`
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
	go garbageCollectionWorker()

	var err error
	webTemplate, err = template.New("webui").Parse(htmlTemplate)
	if err != nil {
		log.Fatalf("Impossible de parser le template HTML: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/api/disk/register", registerVolumeHandler).Methods("POST")
	r.HandleFunc("/api/files/upload", uploadFileHandler).Methods("POST")
	r.HandleFunc("/api/files/download/{filename}", downloadFileHandler).Methods("GET")
	r.HandleFunc("/api/files/delete/{filename}", deleteFileHandler).Methods("POST")
	r.HandleFunc("/api/repair", repairHandler).Methods("POST")
	r.HandleFunc("/api/gc", garbageCollectionHandler).Methods("POST")
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
	indexCopy := make(map[string]*FileMetadata)
	for k, v := range state.FileIndex {
		indexCopy[k] = v
	}
	state.RUnlock()
	
	file, err := os.Create(indexFilePath)
	if err != nil {
		log.Printf("ERREUR: Impossible de sauvegarder l'index: %v", err)
		return
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(&indexCopy); err != nil {
		log.Printf("ERREUR: Impossible d'encoder l'index avec gob: %v", err)
	}
}

func selectVolumesForReplication(count int, excludeDisks []string) ([]*Volume, error) {
	state.RLock()
	defer state.RUnlock()

	var onlineVolumes []*Volume
	for _, v := range state.RegisteredVolumes {
		if v.Status == "En ligne" {
			onlineVolumes = append(onlineVolumes, v)
		}
	}

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

func garbageCollectionWorker() {
	ticker := time.NewTicker(5 * time.Minute) // Garbage collection toutes les 5 minutes
	defer ticker.Stop()
	for range ticker.C {
		runGarbageCollection()
	}
}

func runGarbageCollection() {
	log.Println("Début du garbage collection...")
	
	// Étape 1: Nettoyer les fichiers marqués comme supprimés
	deletedCount := 0
	state.Lock()
	filesToDelete := make([]string, 0)
	for filename, meta := range state.FileIndex {
		if meta.Deleted {
			filesToDelete = append(filesToDelete, filename)
		}
	}
	
	for _, filename := range filesToDelete {
		delete(state.FileIndex, filename)
		deletedCount++
	}
	state.Unlock()
	
	if deletedCount > 0 {
		saveIndex()
		log.Printf("Garbage collection: %d fichiers supprimés de l'index", deletedCount)
	}
	
	// Étape 2: Envoyer les commandes de compactage aux volumes
	state.RLock()
	var onlineVolumes []*Volume
	for _, v := range state.RegisteredVolumes {
		if v.Status == "En ligne" {
			onlineVolumes = append(onlineVolumes, &Volume{
				Name:    v.Name,
				Address: v.Address,
			})
		}
	}
	state.RUnlock()
	
	var wg sync.WaitGroup
	for _, vol := range onlineVolumes {
		wg.Add(1)
		go func(volume *Volume) {
			defer wg.Done()
			compactURL := fmt.Sprintf("http://%s/compact", volume.Address)
			client := &http.Client{Timeout: 30 * time.Second}
			resp, err := client.Post(compactURL, "application/json", nil)
			if err != nil {
				log.Printf("Erreur lors du compactage du volume '%s': %v", volume.Name, err)
				return
			}
			defer resp.Body.Close()
			
			if resp.StatusCode == http.StatusOK {
				log.Printf("Compactage du volume '%s' terminé avec succès", volume.Name)
			} else {
				log.Printf("Erreur lors du compactage du volume '%s': status %d", volume.Name, resp.StatusCode)
			}
		}(vol)
	}
	wg.Wait()
	
	log.Println("Garbage collection terminé")
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
			break
		}
		if readErr == io.ErrUnexpectedEOF {
			buffer = buffer[:bytesRead]
			isLastChunk = true
		} else if readErr != nil {
			http.Error(w, "Erreur de lecture du chunk: "+readErr.Error(), http.StatusInternalServerError)
			return
		}

		hash := sha256.Sum256(buffer)
		checksum := hex.EncodeToString(hash[:])

		targetVolumes, err := selectVolumesForReplication(requiredReplicas, nil)
		if err != nil {
			log.Printf("ERREUR UPLOAD: Impossible de trouver des volumes pour la réplication: %v", err)
			http.Error(w, "Erreur interne: "+err.Error(), http.StatusServiceUnavailable)
			return
		}

		log.Printf("Chunk %d (checksum: %s...): écriture sur %s et %s",
			chunkIdx, checksum[:8], targetVolumes[0].Name, targetVolumes[1].Name)

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
				req.Header.Set("X-Chunk-Checksum", checksum)

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					errs <- fmt.Errorf("volume '%s' injoignable: %w", v.Name, err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(resp.Body)
					errs <- fmt.Errorf("le volume '%s' a refusé l'écriture (status: %d): %s", v.Name, resp.StatusCode, string(body))
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

		fileChunks = append(fileChunks, &ChunkMetadata{
			ChunkIdx: chunkIdx,
			Copies:   successfulCopies,
			Checksum: checksum,
		})
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
	if !ok || meta.Deleted {
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
		expectedChecksum := chunk.Checksum

		for _, copyInfo := range chunk.Copies {
			if copyInfo.Deleted {
				continue // Ignorer les copies marquées comme supprimées
			}
			
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
				lastError = fmt.Errorf("erreur de connexion à '%s': %w", volume.Name, err)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				lastError = fmt.Errorf("échec de lecture sur '%s' (status: %d)", volume.Name, resp.StatusCode)
				continue
			}

			chunkData, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				lastError = fmt.Errorf("erreur de lecture du corps de la réponse de '%s': %w", volume.Name, err)
				continue
			}

			hash := sha256.Sum256(chunkData)
			actualChecksum := hex.EncodeToString(hash[:])

			if actualChecksum != expectedChecksum {
				log.Printf("!!! CORRUPTION DETECTEE !!! Chunk %d de '%s' sur volume '%s'. Attendu: %s, Reçu: %s",
					chunk.ChunkIdx, filename, volume.Name, expectedChecksum, actualChecksum)
				lastError = fmt.Errorf("corruption de données sur '%s'", volume.Name)
				continue
			}

			if _, err := w.Write(chunkData); err != nil {
				log.Printf("Erreur d'envoi du chunk au client: %v", err)
				return
			}

			readSuccessful = true
			break
		}

		if !readSuccessful {
			log.Printf("Échec du téléchargement pour '%s': aucune copie valide du chunk %d n'est disponible. Dernière erreur: %v", filename, chunk.ChunkIdx, lastError)
			http.Error(w, fmt.Sprintf("Fichier corrompu ou indisponible, impossible de lire le chunk %d", chunk.ChunkIdx), http.StatusServiceUnavailable)
			return
		}
	}
}

func deleteFileHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]
	
	// Copier les métadonnées pour éviter de garder le lock trop longtemps
	state.RLock()
	meta, exists := state.FileIndex[filename]
	if !exists {
		state.RUnlock()
		http.NotFound(w, r)
		return
	}
	
	// Créer une copie des chunks pour le traitement en arrière-plan
	var chunksToDelete []struct {
		volumeName string
		offset     uint64
		size       uint32
	}
	
	for _, chunk := range meta.Chunks {
		for _, copy := range chunk.Copies {
			chunksToDelete = append(chunksToDelete, struct {
				volumeName string
				offset     uint64
				size       uint32
			}{copy.VolumeName, copy.Offset, copy.Size})
		}
	}
	state.RUnlock()
	
	// Maintenant marquer comme supprimé avec un lock séparé
	state.Lock()
	if meta, exists := state.FileIndex[filename]; exists {
		meta.Deleted = true
		for _, chunk := range meta.Chunks {
			for _, copy := range chunk.Copies {
				copy.Deleted = true
			}
		}
	}
	state.Unlock()
	
	saveIndex()
	log.Printf("Fichier '%s' marqué pour suppression", filename)
	
	// Envoyer les commandes de suppression aux volumes en arrière-plan
	go func() {
		volumeChunks := make(map[string][]map[string]interface{})
		
		// Grouper les chunks par volume
		for _, chunk := range chunksToDelete {
			volumeChunks[chunk.volumeName] = append(volumeChunks[chunk.volumeName], map[string]interface{}{
				"offset": chunk.offset,
				"size":   chunk.size,
			})
		}
		
		// Envoyer aux volumes en ligne
		state.RLock()
		volumes := make(map[string]*Volume)
		for name, vol := range state.RegisteredVolumes {
			if vol.Status == "En ligne" {
				volumes[name] = vol
			}
		}
		state.RUnlock()
		
		for volumeName, chunks := range volumeChunks {
			if vol, ok := volumes[volumeName]; ok {
				deleteURL := fmt.Sprintf("http://%s/mark_deleted", vol.Address)
				deleteData := map[string]interface{}{
					"chunks": chunks,
				}
				
				jsonData, _ := json.Marshal(deleteData)
				req, _ := http.NewRequest("POST", deleteURL, bytes.NewReader(jsonData))
				req.Header.Set("Content-Type", "application/json")
				
				client := &http.Client{Timeout: 5 * time.Second}
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("Erreur lors de la notification de suppression au volume '%s': %v", volumeName, err)
				} else {
					resp.Body.Close()
					log.Printf("Chunks du fichier '%s' marqués pour suppression sur le volume '%s'", filename, volumeName)
				}
			}
		}
	}()
	
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func repairHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Lancement de la procédure de réparation en arrière-plan...")
	go runRepairProcess()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func garbageCollectionHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Lancement du garbage collection en arrière-plan...")
	go runGarbageCollection()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func runRepairProcess() {
	type repairJob struct {
		filename      string
		chunkIdx      int
		chunkChecksum string
		sourceCopy    *ChunkCopy
		sourceVolume  *Volume
	}
	var jobs []repairJob

	state.RLock()
	for filename, meta := range state.FileIndex {
		if meta.Deleted {
			continue // Ignorer les fichiers supprimés
		}
		for _, chunk := range meta.Chunks {
			var onlineCopies []*ChunkCopy
			for _, copyInfo := range chunk.Copies {
				if !copyInfo.Deleted {
					if vol, ok := state.RegisteredVolumes[copyInfo.VolumeName]; ok && vol.Status == "En ligne" {
						onlineCopies = append(onlineCopies, copyInfo)
					}
				}
			}

			if len(onlineCopies) > 0 && len(onlineCopies) < requiredReplicas {
				sourceCopy := onlineCopies[0]
				sourceVolume := state.RegisteredVolumes[sourceCopy.VolumeName]
				jobs = append(jobs, repairJob{filename, chunk.ChunkIdx, chunk.Checksum, sourceCopy, sourceVolume})
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

	for _, job := range jobs {
		excludeDiskIDs := []string{}
		state.RLock()
		if meta, ok := state.FileIndex[job.filename]; ok && job.chunkIdx < len(meta.Chunks) {
			for _, c := range meta.Chunks[job.chunkIdx].Copies {
				if !c.Deleted {
					if v, ok := state.RegisteredVolumes[c.VolumeName]; ok && v.Status == "En ligne" {
						excludeDiskIDs = append(excludeDiskIDs, v.DiskID)
					}
				}
			}
		} else {
			state.RUnlock()
			continue
		}
		targetVolumes, err := selectVolumesForReplication(1, excludeDiskIDs)
		state.RUnlock()

		if err != nil {
			log.Printf("ERREUR REPARATION: Impossible de trouver un volume de destination pour chunk %d de '%s': %v", job.chunkIdx, job.filename, err)
			continue
		}
		targetVolume := targetVolumes[0]

		readURL := fmt.Sprintf("http://%s/read_chunk?offset=%d&size=%d", job.sourceVolume.Address, job.sourceCopy.Offset, job.sourceCopy.Size)
		resp, err := http.Get(readURL)
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Printf("ERREUR REPARATION: Impossible de lire le chunk source depuis '%s': %v", job.sourceVolume.Name, err)
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		chunkData, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("ERREUR REPARATION: Impossible de lire le corps de la réponse de '%s': %v", job.sourceVolume.Name, err)
			continue
		}

		hash := sha256.Sum256(chunkData)
		actualChecksum := hex.EncodeToString(hash[:])
		if actualChecksum != job.chunkChecksum {
			log.Printf("!!! CORRUPTION DETECTEE PENDANT REPARATION !!! La source du chunk %d ('%s' sur '%s') est corrompue. Annulation de la copie.",
				job.chunkIdx, job.filename, job.sourceVolume.Name)
			continue
		}

		writeURL := fmt.Sprintf("http://%s/write_chunk", targetVolume.Address)
		req, _ := http.NewRequest("POST", writeURL, bytes.NewReader(chunkData))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Chunk-Checksum", job.chunkChecksum)
		writeResp, err := http.DefaultClient.Do(req)
		if err != nil || writeResp.StatusCode != http.StatusOK {
			log.Printf("ERREUR REPARATION: Impossible d'écrire le chunk sur la destination '%s': %v", targetVolume.Name, err)
			if writeResp != nil {
				writeResp.Body.Close()
			}
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

		state.Lock()
		newCopy := &ChunkCopy{
			VolumeName: targetVolume.Name,
			Offset:     writeRespBody.Offset,
			Size:       writeRespBody.Size,
		}
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
	deletedFilesCount := 0

	for _, f := range state.FileIndex {
		if f.Deleted {
			deletedFilesCount++
			continue // Ne pas afficher les fichiers supprimés
		}
		
		isUnavailable := false
		isDegraded := false
		for _, chunk := range f.Chunks {
			onlineCopies := 0
			onlineCopyDisks := make(map[string]bool)
			for _, copyInfo := range chunk.Copies {
				if !copyInfo.Deleted {
					if vol, ok := state.RegisteredVolumes[copyInfo.VolumeName]; ok && vol.Status == "En ligne" {
						onlineCopies++
						onlineCopyDisks[vol.DiskID] = true
					}
				}
			}
			if onlineCopies == 0 {
				isUnavailable = true
				break
			}
			if onlineCopies < requiredReplicas {
				isDegraded = true
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
		CanRepair                   bool
		DeletedFilesCount           int
	}{
		Volumes:                     volumes,
		Files:                       files,
		FilesInDegradedState:        filesInDegradedState,
		NotEnoughDisksForRedundancy: notEnoughDisksForRedundancy,
		RequiredReplicas:            requiredReplicas,
		CanRepair:                   isRepairPossible,
		DeletedFilesCount:           deletedFilesCount,
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
        .alert-info { background-color: #d9edf7; border: 1px solid #bce8f1; color: #31708f; }
        .grid { display: grid; grid-template-columns: 1fr 2fr; gap: 2em; }
        table { width: 100%; border-collapse: collapse; margin-top: 1em; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        .upload-form { background: #f9f9f9; padding: 1.5em; border-radius: 5px; border: 1px solid #ddd; }
        .btn { background-color: #3498db; color: white; padding: 10px 15px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; font-size: 14px; margin-right: 5px; }
        .btn-download { background-color: #27ae60; }
        .btn-delete { background-color: #e74c3c; }
        .btn-repair { background-color: #e67e22; }
        .btn-gc { background-color: #9b59b6; }
		.btn-disabled { background-color: #bdc3c7; cursor: not-allowed; }
        .status-online { color: #27ae60; font-weight: bold; }
        .status-offline { color: #c0392b; font-weight: bold; }
        .status-protected { color: #27ae60; }
        .status-degraded { color: #f39c12; }
        .status-unavailable { color: #c0392b; font-weight: bold; }
        .volume-offline td { color: #95a5a6; }
        .action-buttons { display: flex; gap: 5px; }
        .confirm-delete { background-color: #c0392b; }
    </style>
    <script>
        function confirmDelete(filename) {
            return confirm('Êtes-vous sûr de vouloir supprimer le fichier "' + filename + '" ?\n\nCette action est irréversible.');
        }
    </script>
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
        {{if gt .DeletedFilesCount 0}}
        <div class="alert alert-info">
			<div>
				<strong>Nettoyage disponible !</strong> {{.DeletedFilesCount}} fichier(s) supprimé(s) en attente de nettoyage.
				<br>Lancez le garbage collection pour libérer définitivement l'espace disque.
			</div>
			<div>
				<form action="/api/gc" method="post" style="margin:0;">
					<button type="submit" class="btn btn-gc">
						Lancer le nettoyage
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
                    <thead><tr><th>Nom</th><th>Taille</th><th>Chunks</th><th>Date</th><th>Statut de Redondance</th><th>Actions</th></tr></thead>
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
                                <div class="action-buttons">
                                    <a href="/api/files/download/{{.FileName}}" 
                                       class="btn btn-download {{if eq .Status "Indisponible"}}btn-disabled{{end}}"
                                       {{if eq .Status "Indisponible"}}onclick="return false;"{{end}}>
                                       Télécharger
                                    </a>
                                    <form action="/api/files/delete/{{.FileName}}" method="post" style="display:inline;">
                                        <button type="submit" class="btn btn-delete" onclick="return confirmDelete('{{.FileName}}')">
                                            Supprimer
                                        </button>
                                    </form>
                                </div>
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
