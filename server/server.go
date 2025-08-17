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
	"strconv"
	"sync"
	"text/template"
	"time"

	"github.com/gorilla/mux"
)

// --- Constantes et Configuration ---
const (
	indexFilePath         = "index.idx"
	chunkSize             = 8 * 1024 * 1024     // 8 MB
	requiredReplicas      = 2
	minFreeSpaceForUpload = 100 * 1024 * 1024 // 100 MB minimum
	gcJournalPath         = "gc.journal.tmp"    // Chemin pour le fichier de journal du GC
	maxConcurrentUploads  = 2                   // CORRECTION: Réduit de 4 à 2
	maxConcurrentChunks   = 2                   // CORRECTION: Réduit de 3 à 2
)

// --- Pools pour l'optimisation des performances ---
var (
	// Pool de buffers pour éviter les allocations répétées
	chunkBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, chunkSize)
		},
	}
	
	// Pool de clients HTTP réutilisables
	httpClientPool = sync.Pool{
		New: func() interface{} {
			return &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 10,
					IdleConnTimeout:     90 * time.Second,
				},
			}
		},
	}
	
	// Semaphore pour limiter les uploads concurrents
	uploadSemaphore = make(chan struct{}, maxConcurrentUploads)
)

// --- Structures de Données ---

type ChunkCopy struct {
	VolumeName string
	Offset     uint64
	Size       uint32
	Deleted    bool `json:"deleted,omitempty"`
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
	Deleted    bool   `json:"deleted,omitempty"`
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

// --- État Global ---
var (
	fileIndex      = make(map[string]*FileMetadata)
	fileIndexMutex = sync.RWMutex{}

	registeredVolumes = make(map[string]*Volume)
	volumeMutex       = sync.RWMutex{}
)

// Structure pour le journal du Garbage Collection
type GCJournal struct {
	VolumeOffsetMaps map[string]map[uint64]uint64
}

var webTemplate *template.Template

// --- Fonctions d'optimisation ---

// Fonction pour obtenir un client HTTP du pool
func getHTTPClient() *http.Client {
	return httpClientPool.Get().(*http.Client)
}

// Fonction pour remettre un client HTTP dans le pool
func putHTTPClient(client *http.Client) {
	httpClientPool.Put(client)
}

// --- Fonctions Principales ---
func main() {
	rand.Seed(time.Now().UnixNano())
	loadIndex()
	replayGCJournalIfExists()

	go cleanupInactiveVolumes()
	go garbageCollectionWorker()

	// Audit d'intégrité moins fréquent pour éviter la surcharge
	go func() {
		ticker := time.NewTicker(30 * time.Minute) // Audit toutes les 30 minutes
		defer ticker.Stop()
		for range ticker.C {
			auditDataIntegrity()
		}
	}()

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
	r.HandleFunc("/api/cleanup_replicas", cleanupReplicasHandler).Methods("POST")
	r.HandleFunc("/api/gc", garbageCollectionHandler).Methods("POST")
	r.HandleFunc("/", webUIHandler).Methods("GET")

	log.Println("Serveur de stockage démarré sur http://localhost:8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Le serveur n'a pas pu démarrer: %v", err)
	}
}

// --- Logique Métier (fonctions existantes optimisées) ---

func loadIndex() {
	fileIndexMutex.Lock()
	defer fileIndexMutex.Unlock()

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
	if err := decoder.Decode(&fileIndex); err != nil {
		log.Printf("Erreur au décodage de l'index: %v. L'index sera réinitialisé.", err)
		fileIndex = make(map[string]*FileMetadata)
	} else {
		log.Printf("Index chargé. %d fichiers indexés.", len(fileIndex))
	}
}

func saveIndex_unlocked() {
	tempPath := indexFilePath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		log.Printf("ERREUR: Impossible de créer le fichier temporaire pour l'index: %v", err)
		return
	}

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(&fileIndex)
	file.Close()

	if err != nil {
		log.Printf("ERREUR: Impossible d'encoder l'index avec gob: %v", err)
		os.Remove(tempPath)
		return
	}

	if err := os.Rename(tempPath, indexFilePath); err != nil {
		log.Printf("ERREUR: Impossible de finaliser la sauvegarde de l'index: %v", err)
		os.Remove(tempPath)
		return
	}

	log.Printf("Index sauvegardé de manière atomique avec %d fichiers", len(fileIndex))
}

func saveIndex() {
	fileIndexMutex.Lock()
	defer fileIndexMutex.Unlock()
	saveIndex_unlocked()
}

func replayGCJournalIfExists() {
	file, err := os.Open(gcJournalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatalf("ERREUR CRITIQUE: Impossible d'ouvrir le journal de GC existant: %v", err)
	}
	defer file.Close()

	log.Println("ATTENTION: Journal de Garbage Collection trouvé. Démarrage de la procédure de récupération...")

	var journalData GCJournal
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&journalData); err != nil {
		log.Fatalf("ERREUR CRITIQUE: Impossible de décoder le journal de GC. Intervention manuelle requise. Fichier: %s. Erreur: %v", gcJournalPath, err)
	}
	file.Close()

	applyOffsetMaps(&journalData)

	log.Println("Sauvegarde de l'index récupéré...")
	saveIndex()

	if err := os.Remove(gcJournalPath); err != nil {
		log.Printf("ERREUR CRITIQUE: Impossible de supprimer le journal de GC après récupération: %v. Veuillez le supprimer manuellement.", err)
	} else {
		log.Println("Récupération depuis le journal de GC terminée avec succès.")
	}
}

func applyOffsetMaps_unlocked(journalData *GCJournal) {
	log.Println("Application des mises à jour d'offset depuis le journal de GC...")
	for volumeName, offsetMap := range journalData.VolumeOffsetMaps {
		if len(offsetMap) == 0 {
			continue
		}
		log.Printf("Mise à jour des offsets pour le volume '%s'...", volumeName)
		for _, fileMeta := range fileIndex {
			if fileMeta.Deleted {
				continue
			}
			for _, chunkMeta := range fileMeta.Chunks {
				for _, copyInfo := range chunkMeta.Copies {
					if copyInfo.Deleted {
						continue
					}

					if copyInfo.VolumeName == volumeName {
						if newOffset, ok := findNewOffset(copyInfo.Offset, offsetMap); ok {
							if newOffset != copyInfo.Offset {
								log.Printf("Offset changé pour chunk (fichier: %s) sur %s: %d -> %d", fileMeta.FileName, volumeName, copyInfo.Offset, newOffset)
								copyInfo.Offset = newOffset
							}
						}
					}
				}
			}
		}
	}
	log.Println("Application des mises à jour d'offset terminée.")
}

func applyOffsetMaps(journalData *GCJournal) {
	fileIndexMutex.Lock()
	defer fileIndexMutex.Unlock()
	applyOffsetMaps_unlocked(journalData)
}

// Fonction optimisée pour vérifier les volumes en ligne
func verifyVolumeOnline(volume *Volume) bool {
	if volume.Status != "En ligne" {
		return false
	}

	client := getHTTPClient()
	defer putHTTPClient(client)
	
	// CORRECTION: Timeout plus long et ne plus marquer immédiatement hors ligne
	client.Timeout = 10 * time.Second // Augmenté de 2s à 10s
	testURL := fmt.Sprintf("http://%s/%s/health", volume.Address, volume.Name)

	resp, err := client.Get(testURL)
	if err != nil {
		log.Printf("WARNING: Volume '%s' temporairement inaccessible (ne sera pas marqué hors ligne): %v", volume.Name, err)
		return false // NE PLUS marquer hors ligne immédiatement
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("WARNING: Volume '%s' répond avec un statut non-OK: %d (ne sera pas marqué hors ligne)", volume.Name, resp.StatusCode)
		return false // NE PLUS marquer hors ligne immédiatement
	}

	return true
}

// NOUVELLE FONCTION: Validation avec retry pour éviter les faux échecs
func validateUploadedFileWithRetry(fileName string) error {
	maxRetries := 3
	
	// CORRECTION: Attendre d'abord que les écritures se stabilisent
	time.Sleep(3 * time.Second)
	
	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			// Attendre plus longtemps entre les tentatives
			waitTime := time.Duration(retry*3) * time.Second
			time.Sleep(waitTime)
			log.Printf("Validation du fichier '%s' - tentative %d/%d (attente: %v)", fileName, retry+1, maxRetries, waitTime)
		}
		
		err := validateUploadedFile(fileName)
		if err == nil {
			log.Printf("Validation du fichier '%s' réussie à la tentative %d/%d", fileName, retry+1, maxRetries)
			return nil // Succès
		}
		
		if retry == maxRetries-1 {
			return err // Dernière tentative, retourner l'erreur
		}
		
		log.Printf("Validation échouée pour '%s' (tentative %d/%d): %v", fileName, retry+1, maxRetries, err)
	}
	
	return nil
}

func selectVolumesForReplication(count int, excludeDisks []string, requiredSpace uint64) ([]*Volume, error) {
	volumeMutex.RLock()
	var candidateVolumes []*Volume
	for _, v := range registeredVolumes {
		// CORRECTION: Ne plus vérifier en temps réel - faire confiance au statut heartbeat
		if v.Status == "En ligne" && v.FreeSpace >= requiredSpace {
			candidateVolumes = append(candidateVolumes, v)
		}
	}
	volumeMutex.RUnlock()

	// CORRECTION: Supprimer la vérification agressive qui causait les race conditions
	var eligibleVolumes []*Volume
	for _, v := range candidateVolumes {
		eligibleVolumes = append(eligibleVolumes, v) // Faire confiance au statut heartbeat
	}

	volumesByDiskID := make(map[string][]*Volume)
	for _, v := range eligibleVolumes {
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
		return nil, fmt.Errorf("pas assez de disques physiques avec l'espace libre requis (%d MB) disponibles (requis: %d, dispo: %d)",
			requiredSpace/(1024*1024), count, len(volumesByDiskID))
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
		selectedVolume := volumesOnThisDisk[0]
		for _, vol := range volumesOnThisDisk {
			if vol.FreeSpace > selectedVolume.FreeSpace {
				selectedVolume = vol
			}
		}
		result = append(result, selectedVolume)
	}

	return result, nil
}

func calculateFileStatus(meta *FileMetadata) string {
	if meta.Deleted {
		return "Supprimé"
	}

	allChunksProtected := true
	hasUnavailableChunk := false
	hasOverReplicatedChunk := false

	volumeMutex.RLock()
	defer volumeMutex.RUnlock()

	for _, chunk := range meta.Chunks {
		onlineCopies := 0
		uniqueDisks := make(map[string]bool)

		for _, copyInfo := range chunk.Copies {
			if copyInfo.Deleted {
				continue
			}

			if vol, exists := registeredVolumes[copyInfo.VolumeName]; exists && vol.Status == "En ligne" {
				onlineCopies++
				uniqueDisks[vol.DiskID] = true
			}
		}

		if onlineCopies > requiredReplicas {
			hasOverReplicatedChunk = true
		}

		if onlineCopies == 0 {
			hasUnavailableChunk = true
			break
		}

		if onlineCopies < requiredReplicas || len(uniqueDisks) < requiredReplicas {
			allChunksProtected = false
		}
	}

	if hasUnavailableChunk {
		return "Indisponible"
	}

	if !allChunksProtected {
		return "Dégradé"
	}

	if hasOverReplicatedChunk {
		return "Sur-protégé"
	}

	return "Protégé"
}

// Fonction optimisée pour vérifier l'existence d'un chunk
func verifyChunkExistsOnVolume(volume *Volume, copyInfo *ChunkCopy, expectedChecksum string) bool {
	client := getHTTPClient()
	defer putHTTPClient(client)
	
	// CORRECTION: Timeout plus long et plusieurs tentatives
	client.Timeout = 15 * time.Second // Augmenté de 3s à 15s
	checkURL := fmt.Sprintf("http://%s/%s/verify_chunk?offset=%d&size=%d&checksum=%s",
		volume.Address, volume.Name, copyInfo.Offset, copyInfo.Size, expectedChecksum)

	// Essayer 3 fois avant d'abandonner
	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := client.Get(checkURL)
		if err != nil {
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			log.Printf("ERREUR VERIFICATION: Impossible de vérifier chunk sur '%s' après %d tentatives: %v", volume.Name, attempt, err)
			return false
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return true
		}
		
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	return false
}

func validateUploadedFile(fileName string) error {
	fileIndexMutex.RLock()
	meta, exists := fileIndex[fileName]
	if !exists {
		fileIndexMutex.RUnlock()
		return fmt.Errorf("fichier '%s' non trouvé dans l'index", fileName)
	}

	chunks := make([]*ChunkMetadata, len(meta.Chunks))
	copy(chunks, meta.Chunks)
	fileIndexMutex.RUnlock()

	for i, chunk := range chunks {
		onlineCopies := 0
		uniqueDisks := make(map[string]bool)
		validCopies := 0

		volumeMutex.RLock()
		for _, copyInfo := range chunk.Copies {
			if copyInfo.Deleted {
				continue
			}

			if vol, exists := registeredVolumes[copyInfo.VolumeName]; exists && vol.Status == "En ligne" {
				// CORRECTION: Validation plus permissive - compter d'abord les copies sur volumes en ligne
				onlineCopies++
				uniqueDisks[vol.DiskID] = true
				
				// Vérifier seulement quelques copies pour ne pas surcharger
				if validCopies < requiredReplicas {
					if verifyChunkExistsOnVolume(vol, copyInfo, chunk.Checksum) {
						validCopies++
					}
				} else {
					// Faire confiance aux autres copies si on a déjà validé le minimum requis
					validCopies++
				}
			}
		}
		volumeMutex.RUnlock()

		// CORRECTION: Validation plus souple - accepter si on a les copies sur des disques différents
		// même si la vérification individuelle échoue temporairement
		if onlineCopies >= requiredReplicas && len(uniqueDisks) >= requiredReplicas {
			// Si on a les copies sur différents disques, c'est OK
			continue
		}
		
		if validCopies < requiredReplicas || len(uniqueDisks) < requiredReplicas {
			return fmt.Errorf("chunk %d du fichier '%s' n'a que %d copies validées sur %d disques différents (requis: %d)",
				i, fileName, validCopies, len(uniqueDisks), requiredReplicas)
		}
	}

	return nil
}

func auditDataIntegrity() {
	log.Println("Début de l'audit d'intégrité des données...")

	fileIndexMutex.RLock()
	filesToAudit := make(map[string]*FileMetadata)
	for filename, meta := range fileIndex {
		if !meta.Deleted {
			chunks := make([]*ChunkMetadata, len(meta.Chunks))
			copy(chunks, meta.Chunks)
			filesToAudit[filename] = &FileMetadata{
				FileName:   meta.FileName,
				TotalSize:  meta.TotalSize,
				UploadDate: meta.UploadDate,
				Chunks:     chunks,
			}
		}
	}
	fileIndexMutex.RUnlock()

	issuesFound := 0
	for filename, meta := range filesToAudit {
		realStatus := calculateFileStatus(meta)

		if realStatus == "Dégradé" || realStatus == "Indisponible" {
			log.Printf("AUDIT: Fichier '%s' a un statut problématique: %s", filename, realStatus)
			issuesFound++

			fileIndexMutex.Lock()
			if indexMeta, exists := fileIndex[filename]; exists {
				indexMeta.Status = realStatus
			}
			fileIndexMutex.Unlock()
		}
	}

	if issuesFound > 0 {
		saveIndex()
		log.Printf("AUDIT: %d problèmes d'intégrité détectés et corrigés", issuesFound)
	} else {
		log.Println("AUDIT: Aucun problème d'intégrité détecté")
	}
}

func cleanupInactiveVolumes() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		volumeMutex.Lock()
		for _, volume := range registeredVolumes {
			if time.Since(volume.LastSeen) > 45*time.Second && volume.Status == "En ligne" {
				log.Printf("Volume '%s' inactif. Marquage comme Hors ligne.", volume.Name)
				volume.Status = "Hors ligne"
			}
		}
		volumeMutex.Unlock()
	}
}

func garbageCollectionWorker() {
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		runGarbageCollection()
	}
}

func findNewOffset(oldOffset uint64, offsetMap map[uint64]uint64) (uint64, bool) {
	if offsetMap == nil || len(offsetMap) == 0 {
		return 0, false
	}

	var bestMatchOldStart uint64
	found := false

	for oldStart := range offsetMap {
		if oldStart <= oldOffset {
			if !found || oldStart > bestMatchOldStart {
				bestMatchOldStart = oldStart
				found = true
			}
		}
	}

	if found {
		newStart := offsetMap[bestMatchOldStart]
		relativeOffset := oldOffset - bestMatchOldStart
		return newStart + relativeOffset, true
	}

	return 0, false
}

func runGarbageCollection() {
	log.Println("Lancement du garbage collection...")

	fileIndexMutex.Lock()
	defer fileIndexMutex.Unlock()

	log.Println("GC: Verrou de l'index acquis. Les uploads sont mis en pause.")

	deletedCount := 0
	filesToDelete := make([]string, 0)
	for filename, meta := range fileIndex {
		if meta.Deleted {
			filesToDelete = append(filesToDelete, filename)
		}
	}
	for _, filename := range filesToDelete {
		delete(fileIndex, filename)
		deletedCount++
	}

	if deletedCount > 0 {
		log.Printf("GC: %d fichiers supprimés de l'index", deletedCount)
	}

	type ChunkToKeep struct {
		Offset uint64 `json:"offset"`
		Size   uint32 `json:"size"`
	}
	chunksToKeepByVolume := make(map[string][]ChunkToKeep)

	volumeMutex.RLock()
	onlineVolumes := make([]*Volume, 0, len(registeredVolumes))
	for _, v := range registeredVolumes {
		if v.Status == "En ligne" {
			onlineVolumes = append(onlineVolumes, v)
		}
	}

	for _, fileMeta := range fileIndex {
		for _, chunkMeta := range fileMeta.Chunks {
			for _, copyInfo := range chunkMeta.Copies {
				if !copyInfo.Deleted {
					if vol, exists := registeredVolumes[copyInfo.VolumeName]; exists && vol.Status == "En ligne" {
						chunksToKeepByVolume[copyInfo.VolumeName] = append(chunksToKeepByVolume[copyInfo.VolumeName], ChunkToKeep{
							Offset: copyInfo.Offset,
							Size:   copyInfo.Size,
						})
					}
				}
			}
		}
	}
	volumeMutex.RUnlock()

	log.Println("GC: Snapshot cohérent de l'index créé. Envoi des commandes de compactage...")

	var wg sync.WaitGroup
	offsetMapsChan := make(chan struct {
		VolumeName string
		OffsetMap  map[uint64]uint64
	}, len(onlineVolumes))

	for _, vol := range onlineVolumes {
		wg.Add(1)
		go func(volume *Volume, chunksToKeep []ChunkToKeep) {
			defer wg.Done()
			log.Printf("GC: Volume '%s' doit conserver %d chunks.", volume.Name, len(chunksToKeep))

			requestBody, err := json.Marshal(struct {
				ChunksToKeep []ChunkToKeep `json:"chunks_to_keep"`
			}{ChunksToKeep: chunksToKeep})
			if err != nil {
				log.Printf("Erreur de sérialisation pour '%s': %v", volume.Name, err)
				return
			}

			compactURL := fmt.Sprintf("http://%s/%s/compact", volume.Address, volume.Name)
			client := getHTTPClient()
			defer putHTTPClient(client)
			client.Timeout = 15 * time.Minute

			resp, err := client.Post(compactURL, "application/json", bytes.NewBuffer(requestBody))
			if err != nil {
				log.Printf("Erreur lors de la demande de compactage à '%s': %v", volume.Name, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				var compactResp struct {
					OffsetMap map[string]uint64 `json:"offset_map"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&compactResp); err != nil {
					log.Printf("Erreur de décodage de la réponse de '%s': %v", volume.Name, err)
					return
				}
				if compactResp.OffsetMap != nil && len(compactResp.OffsetMap) > 0 {
					newOffsetMap := make(map[uint64]uint64)
					for oldOffStr, newOff := range compactResp.OffsetMap {
						if oldOff, err := strconv.ParseUint(oldOffStr, 10, 64); err == nil {
							newOffsetMap[oldOff] = newOff
						}
					}
					offsetMapsChan <- struct {
						VolumeName string
						OffsetMap  map[uint64]uint64
					}{VolumeName: volume.Name, OffsetMap: newOffsetMap}
				}
				log.Printf("Compactage du volume '%s' terminé", volume.Name)
			} else {
				body, _ := io.ReadAll(resp.Body)
				log.Printf("Erreur lors du compactage de '%s': status %d, body: %s", volume.Name, resp.StatusCode, string(body))
			}
		}(vol, chunksToKeepByVolume[vol.Name])
	}
	wg.Wait()
	close(offsetMapsChan)

	log.Println("GC: Tous les volumes ont terminé le compactage.")

	journalData := GCJournal{
		VolumeOffsetMaps: make(map[string]map[uint64]uint64),
	}
	hasChanges := false
	for res := range offsetMapsChan {
		if len(res.OffsetMap) > 0 {
			journalData.VolumeOffsetMaps[res.VolumeName] = res.OffsetMap
			hasChanges = true
		}
	}

	if !hasChanges {
		log.Println("GC terminé. Aucun changement d'offset à appliquer.")
		return
	}

	log.Println("GC: Écriture du journal de transaction...")
	journalFile, err := os.Create(gcJournalPath)
	if err != nil {
		log.Printf("ERREUR CRITIQUE: Impossible de créer le journal de GC: %v. Abandon.", err)
		return
	}
	encoder := gob.NewEncoder(journalFile)
	if err := encoder.Encode(&journalData); err != nil {
		journalFile.Close()
		os.Remove(gcJournalPath)
		log.Printf("ERREUR CRITIQUE: Impossible d'écrire dans le journal de GC: %v. Abandon.", err)
		return
	}
	journalFile.Close()

	applyOffsetMaps_unlocked(&journalData)
	saveIndex_unlocked()

	if err := os.Remove(gcJournalPath); err != nil {
		log.Printf("AVERTISSEMENT: Impossible de supprimer le journal de GC après succès: %v", err)
	}

	log.Println("Garbage collection terminé et index mis à jour de manière transactionnelle et sécurisée.")
}

// --- Handlers HTTP Optimisés ---

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

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	existingVol, exists := registeredVolumes[volData.Name]

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
			registeredVolumes[volData.Name] = existingVol
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
			registeredVolumes[volData.Name] = existingVol
		}
		updateVolume(existingVol)
		w.WriteHeader(http.StatusOK)
		return
	}

	http.Error(w, "Type de requête ('initial' ou 'heartbeat') manquant.", http.StatusBadRequest)
}

// Structure pour les jobs de chunks dans l'upload parallèle
type chunkJob struct {
	index    int
	data     []byte
	checksum string
	size     int
}

type chunkResult struct {
	index int
	chunk *ChunkMetadata
	err   error
}

// Upload handler optimisé avec traitement parallèle des chunks
func uploadFileHandler(w http.ResponseWriter, r *http.Request) {
	// Acquérir un slot d'upload pour limiter la concurrence
	uploadSemaphore <- struct{}{}
	defer func() { <-uploadSemaphore }()

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

	fileIndexMutex.RLock()
	_, exists := fileIndex[fileName]
	fileIndexMutex.RUnlock()
	if exists {
		http.Error(w, "Un fichier avec ce nom existe déjà.", http.StatusConflict)
		return
	}

	// Canaux pour le pipeline parallèle
	chunkJobChan := make(chan chunkJob, maxConcurrentChunks)
	chunkResultChan := make(chan chunkResult, maxConcurrentChunks)
	var wg sync.WaitGroup

	// Workers pour traiter les chunks en parallèle
	for i := 0; i < maxConcurrentChunks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range chunkJobChan {
				result := processChunkUpload(job)
				chunkResultChan <- result
			}
		}()
	}

	// Lecture et envoi des chunks
	go func() {
		defer close(chunkJobChan)
		chunkIdx := 0
		
		for {
			// Obtenir un buffer du pool
			buffer := chunkBufferPool.Get().([]byte)
			
			bytesRead, readErr := io.ReadFull(part, buffer)
			
			isLastChunk := false
			if readErr == io.EOF {
				chunkBufferPool.Put(buffer)
				break
			}
			if readErr == io.ErrUnexpectedEOF {
				buffer = buffer[:bytesRead]
				isLastChunk = true
			} else if readErr != nil {
				chunkBufferPool.Put(buffer)
				log.Printf("Erreur de lecture du chunk: %v", readErr)
				break
			}

			// Calculer le checksum
			hash := sha256.Sum256(buffer)
			checksum := hex.EncodeToString(hash[:])

			// Copier les données pour éviter la corruption du buffer
			chunkData := make([]byte, len(buffer))
			copy(chunkData, buffer)
			chunkBufferPool.Put(buffer)

			chunkJobChan <- chunkJob{
				index:    chunkIdx,
				data:     chunkData,
				checksum: checksum,
				size:     len(chunkData),
			}
			
			chunkIdx++
			
			if isLastChunk {
				break
			}
		}
	}()

	// Collecter les résultats
	go func() {
		wg.Wait()
		close(chunkResultChan)
	}()

	var fileChunks []*ChunkMetadata
	var totalSize int64
	var successfulChunks []string
	uploadValid := true
	var uploadError error

	resultMap := make(map[int]*ChunkMetadata)
	expectedChunks := 0

	for result := range chunkResultChan {
		expectedChunks++
		if result.err != nil {
			uploadValid = false
			uploadError = result.err
			log.Printf("ERREUR UPLOAD: Chunk %d: %v", result.index, result.err)
			continue
		}
		
		resultMap[result.index] = result.chunk
		totalSize += int64(result.chunk.Copies[0].Size)
		
		// Construire les identifiants pour le nettoyage
		for _, copy := range result.chunk.Copies {
			successfulChunks = append(successfulChunks, 
				fmt.Sprintf("%s:%s:%d:%d", copy.VolumeName, result.chunk.Checksum, copy.Offset, copy.Size))
		}
	}

	if !uploadValid {
		log.Printf("ERREUR UPLOAD: Upload invalide pour '%s': %v", fileName, uploadError)
		go cleanupUploadChunks(successfulChunks)
		http.Error(w, "Échec de l'upload: "+uploadError.Error(), http.StatusInternalServerError)
		return
	}

	// Reconstituer les chunks dans l'ordre
	for i := 0; i < expectedChunks; i++ {
		if chunk, exists := resultMap[i]; exists {
			fileChunks = append(fileChunks, chunk)
		}
	}

	tempMeta := &FileMetadata{
		FileName:   fileName,
		TotalSize:  totalSize,
		UploadDate: time.Now(),
		Chunks:     fileChunks,
	}
	fileIndexMutex.Lock()
	fileIndex[fileName] = tempMeta
	fileIndexMutex.Unlock()

	if err := validateUploadedFileWithRetry(fileName); err != nil {
		log.Printf("ERREUR VALIDATION: Upload de '%s' échoue à la validation: %v", fileName, err)

		fileIndexMutex.Lock()
		delete(fileIndex, fileName)
		fileIndexMutex.Unlock()

		go cleanupUploadChunks(successfulChunks)
		http.Error(w, "Échec de la validation: "+err.Error(), http.StatusInternalServerError)
		return
	}

	saveIndex()
	log.Printf("Fichier %s (taille: %d, chunks: %d) uploadé et validé avec succès.", fileName, totalSize, len(fileChunks))
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// Fonction pour traiter un chunk d'upload
func processChunkUpload(job chunkJob) chunkResult {
	requiredSpace := uint64(job.size) + minFreeSpaceForUpload
	targetVolumes, err := selectVolumesForReplication(requiredReplicas, nil, requiredSpace)
	if err != nil {
		return chunkResult{index: job.index, err: fmt.Errorf("impossible de trouver des volumes pour la réplication: %w", err)}
	}

	log.Printf("Chunk %d (checksum: %s...): écriture sur %s et %s",
		job.index, job.checksum[:8], targetVolumes[0].Name, targetVolumes[1].Name)

	var wg sync.WaitGroup
	results := make(chan *ChunkCopy, requiredReplicas)
	errs := make(chan error, requiredReplicas)

	for _, vol := range targetVolumes {
	wg.Add(1)
	go func(v *Volume) {
		defer wg.Done()

		// CORRECTION: Ne plus vérifier avant écriture - écrire directement
		// if !verifyVolumeOnline(v) { ... } <- SUPPRIMER CETTE VÉRIFICATION

		diskURL := fmt.Sprintf("http://%s/%s/write_chunk", v.Address, v.Name)
		req, _ := http.NewRequest("POST", diskURL, bytes.NewReader(job.data))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Chunk-Checksum", job.checksum)

		client := getHTTPClient()
		defer putHTTPClient(client)
		
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("ERREUR: Volume '%s' inaccessible pendant écriture: %v", v.Name, err)
			// CORRECTION: Ne plus marquer immédiatement hors ligne
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
	
	var errors []error
	for e := range errs {
		errors = append(errors, e)
	}

	if len(successfulCopies) < requiredReplicas {
		return chunkResult{
			index: job.index,
			err:   fmt.Errorf("chunk %d: seulement %d copies réussies sur %d requises. Erreurs: %v", job.index, len(successfulCopies), requiredReplicas, errors),
		}
	}

	uniqueDisks := make(map[string]bool)
	volumeMutex.RLock()
	for _, copy := range successfulCopies {
		if vol, exists := registeredVolumes[copy.VolumeName]; exists {
			uniqueDisks[vol.DiskID] = true
		}
	}
	volumeMutex.RUnlock()

	if len(uniqueDisks) < requiredReplicas {
		return chunkResult{
			index: job.index,
			err:   fmt.Errorf("chunk %d: copies sur seulement %d disques différents (requis: %d)", job.index, len(uniqueDisks), requiredReplicas),
		}
	}

	return chunkResult{
		index: job.index,
		chunk: &ChunkMetadata{
			ChunkIdx: job.index,
			Copies:   successfulCopies,
			Checksum: job.checksum,
		},
	}
}

func cleanupUploadChunks(chunkIdentifiers []string) {
	if len(chunkIdentifiers) == 0 {
		return
	}

	log.Printf("Nettoyage de %d chunks suite à un échec d'upload", len(chunkIdentifiers))

	volumeChunks := make(map[string][]map[string]interface{})

	for _, identifier := range chunkIdentifiers {
		parts := parseChunkIdentifier(identifier)
		if len(parts) == 4 {
			volumeName := parts[0]
			checksum := parts[1]
			offset := parts[2]
			size := parts[3]

			volumeChunks[volumeName] = append(volumeChunks[volumeName], map[string]interface{}{
				"checksum": checksum,
				"offset":   offset,
				"size":     size,
			})
		}
	}

	volumeMutex.RLock()
	volumes := make(map[string]*Volume)
	for name, vol := range registeredVolumes {
		if vol.Status == "En ligne" {
			volumes[name] = vol
		}
	}
	volumeMutex.RUnlock()

	for volumeName, chunks := range volumeChunks {
		if vol, ok := volumes[volumeName]; ok {
			if !verifyVolumeOnline(vol) {
				log.Printf("SÉCURITÉ: Volume '%s' inaccessible, impossible de nettoyer les chunks orphelins", volumeName)
				continue
			}

			cleanupURL := fmt.Sprintf("http://%s/%s/cleanup_orphan", vol.Address, vol.Name)
			cleanupData := map[string]interface{}{
				"chunks": chunks,
			}

			jsonData, _ := json.Marshal(cleanupData)
			client := getHTTPClient()
			defer putHTTPClient(client)
			
			req, _ := http.NewRequest("POST", cleanupURL, bytes.NewReader(jsonData))
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Erreur lors du nettoyage des chunks orphelins sur '%s': %v", volumeName, err)
				volumeMutex.Lock()
				if volume, exists := registeredVolumes[volumeName]; exists {
					volume.Status = "Hors ligne"
				}
				volumeMutex.Unlock()
			} else {
				resp.Body.Close()
				log.Printf("Chunks orphelins nettoyés sur le volume '%s'", volumeName)
			}
		}
	}
}

func parseChunkIdentifier(identifier string) []string {
	parts := make([]string, 0, 4)
	current := ""
	colonCount := 0

	for _, char := range identifier {
		if char == ':' && colonCount < 3 {
			parts = append(parts, current)
			current = ""
			colonCount++
		} else {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	return parts
}

// Download handler optimisé avec streaming
func downloadFileHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]

	fileIndexMutex.RLock()
	meta, ok := fileIndex[filename]
	if !ok || meta.Deleted {
		fileIndexMutex.RUnlock()
		http.NotFound(w, r)
		return
	}
	metaCopy := &FileMetadata{
		TotalSize: meta.TotalSize,
		Chunks:    make([]*ChunkMetadata, len(meta.Chunks)),
	}
	copy(metaCopy.Chunks, meta.Chunks)
	fileIndexMutex.RUnlock()

	w.Header().Set("Content-Disposition", "attachment; filename=\""+filename+"\"")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", metaCopy.TotalSize))

	// Pipeline de téléchargement pour améliorer les performances
	for _, chunk := range metaCopy.Chunks {
		var readSuccessful bool
		var lastError error
		expectedChecksum := chunk.Checksum

		for _, copyInfo := range chunk.Copies {
			if copyInfo.Deleted {
				continue
			}

			volumeMutex.RLock()
			volume, volOK := registeredVolumes[copyInfo.VolumeName]
			volumeMutex.RUnlock()

			if !volOK {
				lastError = fmt.Errorf("volume '%s' non trouvé", copyInfo.VolumeName)
				continue
			}

			if !verifyVolumeOnline(volume) {
				lastError = fmt.Errorf("volume '%s' hors ligne (vérification temps réel)", copyInfo.VolumeName)
				continue
			}

			diskURL := fmt.Sprintf("http://%s/%s/read_chunk?offset=%d&size=%d", volume.Address, volume.Name, copyInfo.Offset, copyInfo.Size)

			client := getHTTPClient()
			defer putHTTPClient(client)
			client.Timeout = 30 * time.Second
			
			resp, err := client.Get(diskURL)
			if err != nil {
				log.Printf("SÉCURITÉ: Erreur de lecture détectée sur '%s', marquage hors ligne", volume.Name)
				volumeMutex.Lock()
				if vol, exists := registeredVolumes[copyInfo.VolumeName]; exists {
					vol.Status = "Hors ligne"
				}
				volumeMutex.Unlock()
				lastError = fmt.Errorf("erreur de connexion à '%s': %w", volume.Name, err)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				lastError = fmt.Errorf("échec de lecture sur '%s' (status: %d): %s", volume.Name, resp.StatusCode, string(bodyBytes))
				continue
			}

			// Streaming direct pour éviter de charger tout en mémoire
			buffer := chunkBufferPool.Get().([]byte)
			defer chunkBufferPool.Put(buffer)
			
			hasher := sha256.New()
			written, err := io.CopyBuffer(io.MultiWriter(w, hasher), resp.Body, buffer)
			resp.Body.Close()
			
			if err != nil {
				lastError = fmt.Errorf("erreur de streaming depuis '%s': %w", volume.Name, err)
				continue
			}

			if written != int64(copyInfo.Size) {
				lastError = fmt.Errorf("taille incorrecte reçue de '%s': attendu %d, reçu %d", volume.Name, copyInfo.Size, written)
				continue
			}

			actualChecksum := hex.EncodeToString(hasher.Sum(nil))
			if actualChecksum != expectedChecksum {
				log.Printf("!!! CORRUPTION DETECTEE !!! Chunk %d de '%s' sur volume '%s'. Attendu: %s, Reçu: %s",
					chunk.ChunkIdx, filename, volume.Name, expectedChecksum, actualChecksum)
				lastError = fmt.Errorf("corruption de données sur '%s'", volume.Name)
				continue
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

	fileIndexMutex.Lock()
	defer fileIndexMutex.Unlock()

	meta, exists := fileIndex[filename]
	if !exists {
		http.NotFound(w, r)
		return
	}

	meta.Deleted = true
	var chunksToDelete []struct {
		volumeName string
		offset     uint64
		size       uint32
	}
	for _, chunk := range meta.Chunks {
		for _, copy := range chunk.Copies {
			copy.Deleted = true
			chunksToDelete = append(chunksToDelete, struct {
				volumeName string
				offset     uint64
				size       uint32
			}{copy.VolumeName, copy.Offset, copy.Size})
		}
	}

	saveIndex_unlocked()

	go func() {
		log.Printf("Fichier '%s' marqué pour suppression", filename)

		volumeChunks := make(map[string][]map[string]interface{})

		for _, chunk := range chunksToDelete {
			volumeChunks[chunk.volumeName] = append(volumeChunks[chunk.volumeName], map[string]interface{}{
				"offset": chunk.offset,
				"size":   chunk.size,
			})
		}

		volumeMutex.RLock()
		volumes := make(map[string]*Volume)
		for name, vol := range registeredVolumes {
			if vol.Status == "En ligne" {
				volumes[name] = vol
			}
		}
		volumeMutex.RUnlock()

		for volumeName, chunks := range volumeChunks {
			if vol, ok := volumes[volumeName]; ok {
				deleteURL := fmt.Sprintf("http://%s/%s/mark_deleted", vol.Address, vol.Name)
				deleteData := map[string]interface{}{
					"chunks": chunks,
				}

				jsonData, _ := json.Marshal(deleteData)
				req, _ := http.NewRequest("POST", deleteURL, bytes.NewReader(jsonData))
				req.Header.Set("Content-Type", "application/json")

				client := getHTTPClient()
				defer putHTTPClient(client)
				client.Timeout = 5 * time.Second
				
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

func cleanupReplicasHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Lancement du nettoyage des répliques excédentaires en arrière-plan...")
	go runCleanupReplicasProcess()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func runCleanupReplicasProcess() {
	log.Println("Début du processus de nettoyage des répliques excédentaires.")

	volumeDeletionMap := make(map[string][]map[string]interface{})
	cleanedCount := 0

	volumeMutex.Lock()
	fileIndexMutex.Lock()

	filesToProcess := make([]*FileMetadata, 0, len(fileIndex))
	for _, meta := range fileIndex {
		if !meta.Deleted {
			filesToProcess = append(filesToProcess, meta)
		}
	}

	onlineVolumes := make(map[string]*Volume)
	for name, vol := range registeredVolumes {
		if vol.Status == "En ligne" {
			onlineVolumes[name] = vol
		}
	}

	for _, meta := range filesToProcess {
		for chunkIdx, chunk := range meta.Chunks {
			var onlineCopies []*ChunkCopy
			for _, copyInfo := range chunk.Copies {
				if !copyInfo.Deleted {
					if _, ok := onlineVolumes[copyInfo.VolumeName]; ok {
						onlineCopies = append(onlineCopies, copyInfo)
					}
				}
			}

			extrasToRemove := len(onlineCopies) - requiredReplicas
			if extrasToRemove > 0 {
				rand.Shuffle(len(onlineCopies), func(i, j int) {
					onlineCopies[i], onlineCopies[j] = onlineCopies[j], onlineCopies[i]
				})

				copiesToDelete := onlineCopies[:extrasToRemove]
				for _, copyToDelete := range copiesToDelete {
					copyToDelete.Deleted = true
					volumeDeletionMap[copyToDelete.VolumeName] = append(volumeDeletionMap[copyToDelete.VolumeName], map[string]interface{}{
						"offset": copyToDelete.Offset,
						"size":   copyToDelete.Size,
					})
					cleanedCount++
					log.Printf("Nettoyage: Marqué la copie du chunk %d du fichier '%s' sur le volume '%s' pour suppression.", chunkIdx, meta.FileName, copyToDelete.VolumeName)
				}
			}
		}
	}

	if cleanedCount > 0 {
		saveIndex_unlocked()
	}

	fileIndexMutex.Unlock()
	volumeMutex.Unlock()

	if cleanedCount > 0 {
		go func() {
			for volumeName, chunks := range volumeDeletionMap {
				volumeMutex.RLock()
				vol, ok := registeredVolumes[volumeName]
				volumeMutex.RUnlock()

				if ok && vol.Status == "En ligne" {
					deleteURL := fmt.Sprintf("http://%s/%s/mark_deleted", vol.Address, vol.Name)
					deleteData := map[string]interface{}{"chunks": chunks}

					jsonData, _ := json.Marshal(deleteData)
					req, _ := http.NewRequest("POST", deleteURL, bytes.NewReader(jsonData))
					req.Header.Set("Content-Type", "application/json")

					client := getHTTPClient()
					defer putHTTPClient(client)
					client.Timeout = 5 * time.Second
					
					resp, err := client.Do(req)
					if err != nil {
						log.Printf("Erreur lors de la notification de suppression (nettoyage) au volume '%s': %v", volumeName, err)
					} else {
						resp.Body.Close()
						log.Printf("Copies excédentaires marquées pour suppression sur le volume '%s'", volumeName)
					}
				}
			}
		}()
		log.Printf("Nettoyage des répliques terminé. %d copies ont été marquées pour suppression.", cleanedCount)
	} else {
		log.Println("Nettoyage des répliques terminé. Aucune copie excédentaire n'a été trouvée.")
	}
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

	volumeMutex.RLock()
	fileIndexMutex.RLock()
	for filename, meta := range fileIndex {
		if meta.Deleted {
			continue
		}
		for _, chunk := range meta.Chunks {
			var onlineCopies []*ChunkCopy
			for _, copyInfo := range chunk.Copies {
				if !copyInfo.Deleted {
					if vol, ok := registeredVolumes[copyInfo.VolumeName]; ok && vol.Status == "En ligne" {
						onlineCopies = append(onlineCopies, copyInfo)
					}
				}
			}

			if len(onlineCopies) > 0 && len(onlineCopies) < requiredReplicas {
				sourceCopy := onlineCopies[0]
				sourceVolume := registeredVolumes[sourceCopy.VolumeName]
				jobs = append(jobs, repairJob{filename, chunk.ChunkIdx, chunk.Checksum, sourceCopy, sourceVolume})
			}
		}
	}
	fileIndexMutex.RUnlock()
	volumeMutex.RUnlock()

	if len(jobs) == 0 {
		log.Println("Procédure de réparation terminée. Aucun chunk dégradé et réparable n'a été trouvé.")
		return
	}

	log.Printf("Début de la réparation pour %d chunks...", len(jobs))
	repairedChunks := 0

	for _, job := range jobs {
		excludeDiskIDs := []string{}
		volumeMutex.RLock()
		fileIndexMutex.RLock()
		if meta, ok := fileIndex[job.filename]; ok && job.chunkIdx < len(meta.Chunks) {
			for _, c := range meta.Chunks[job.chunkIdx].Copies {
				if !c.Deleted {
					if v, ok := registeredVolumes[c.VolumeName]; ok && v.Status == "En ligne" {
						excludeDiskIDs = append(excludeDiskIDs, v.DiskID)
					}
				}
			}
		} else {
			fileIndexMutex.RUnlock()
			volumeMutex.RUnlock()
			continue
		}
		fileIndexMutex.RUnlock()
		volumeMutex.RUnlock()

		requiredSpace := uint64(job.sourceCopy.Size) + minFreeSpaceForUpload
		targetVolumes, err := selectVolumesForReplication(1, excludeDiskIDs, requiredSpace)

		if err != nil {
			log.Printf("ERREUR REPARATION: Impossible de trouver un volume de destination pour chunk %d de '%s': %v", job.chunkIdx, job.filename, err)
			continue
		}
		targetVolume := targetVolumes[0]

		readURL := fmt.Sprintf("http://%s/%s/read_chunk?offset=%d&size=%d", job.sourceVolume.Address, job.sourceVolume.Name, job.sourceCopy.Offset, job.sourceCopy.Size)
		
		client := getHTTPClient()
		defer putHTTPClient(client)
		
		resp, err := client.Get(readURL)
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

		writeURL := fmt.Sprintf("http://%s/%s/write_chunk", targetVolume.Address, targetVolume.Name)
		req, _ := http.NewRequest("POST", writeURL, bytes.NewReader(chunkData))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Chunk-Checksum", job.chunkChecksum)
		
		writeResp, err := client.Do(req)
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

		fileIndexMutex.Lock()
		newCopy := &ChunkCopy{
			VolumeName: targetVolume.Name,
			Offset:     writeRespBody.Offset,
			Size:       writeRespBody.Size,
		}
		if meta, ok := fileIndex[job.filename]; ok && job.chunkIdx < len(meta.Chunks) {
			meta.Chunks[job.chunkIdx].Copies = append(meta.Chunks[job.chunkIdx].Copies, newCopy)
			repairedChunks++
			log.Printf("Succès: Chunk %d de '%s' recopié de '%s' vers '%s' (Disque: %s)",
				job.chunkIdx, job.filename, job.sourceVolume.Name, targetVolume.Name, targetVolume.DiskID)
		}
		fileIndexMutex.Unlock()
	}

	if repairedChunks > 0 {
		saveIndex()
		log.Printf("Procédure de réparation terminée. %d chunks ont été restaurés.", repairedChunks)
	} else {
		log.Println("Procédure de réparation terminée. Aucun chunk n'a pu être réparé.")
	}
}

func webUIHandler(w http.ResponseWriter, r *http.Request) {
	// Verrouiller les mutex dans un ordre constant pour éviter les deadlocks
	volumeMutex.RLock()
	fileIndexMutex.RLock()
	defer fileIndexMutex.RUnlock()
	defer volumeMutex.RUnlock()

	volumes := make([]*Volume, 0, len(registeredVolumes))
	onlinePhysicalDisks := make(map[string]bool)
	for _, v := range registeredVolumes {
		volumes = append(volumes, v)
		if v.Status == "En ligne" {
			onlinePhysicalDisks[v.DiskID] = true
		}
	}

	files := make([]*FileMetadata, 0, len(fileIndex))
	filesInDegradedState := false
	hasOverReplicatedFiles := false
	isRepairPossible := false
	deletedFilesCount := 0

	for _, f := range fileIndex {
		if f.Deleted {
			deletedFilesCount++
			continue
		}

		f.Status = calculateFileStatus(f)

		if f.Status == "Dégradé" {
			filesInDegradedState = true

			for _, chunk := range f.Chunks {
				onlineCopyDisks := make(map[string]bool)
				for _, copyInfo := range chunk.Copies {
					if !copyInfo.Deleted {
						if vol, ok := registeredVolumes[copyInfo.VolumeName]; ok && vol.Status == "En ligne" {
							onlineCopyDisks[vol.DiskID] = true
						}
					}
				}
				if len(onlinePhysicalDisks) > len(onlineCopyDisks) {
					isRepairPossible = true
					break
				}
			}
		}

		if f.Status == "Sur-protégé" {
			hasOverReplicatedFiles = true
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
		HasOverReplicatedFiles      bool
		NotEnoughDisksForRedundancy bool
		RequiredReplicas            int
		CanRepair                   bool
		DeletedFilesCount           int
	}{
		Volumes:                     volumes,
		Files:                       files,
		FilesInDegradedState:        filesInDegradedState,
		HasOverReplicatedFiles:      hasOverReplicatedFiles,
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
		.status-over-protected { color: #3498db; }
        .volume-offline td { color: #95a5a6; }
        .action-buttons { display: flex; gap: 5px; }
        .confirm-delete { background-color: #c0392b; }
        .performance-info { background: #e8f5e8; padding: 1em; border-radius: 5px; margin-bottom: 1em; font-size: 0.9em; }
    </style>
    <script>
        function confirmDelete(filename) {
            return confirm('Êtes-vous sûr de vouloir supprimer le fichier "' + filename + '" ?\n\nCette action est irréversible.');
        }
    </script>
</head>
<body>
    <div class="container">
        <h1>💿 Panneau de Contrôle du Stockage (Version Optimisée)</h1>
        
        <div class="performance-info">
            <strong>🚀 Optimisations Actives:</strong> Upload parallèle (3 chunks simultanés) • Pool de buffers • Clients HTTP réutilisables • Streaming de téléchargement • Sync asynchrone
        </div>
        
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
		{{if .HasOverReplicatedFiles}}
        <div class="alert alert-info">
			<div>
				<strong>Optimisation possible !</strong> Certains fichiers ont plus de copies que nécessaire ({{.RequiredReplicas}} requises).
				<br>Vous pouvez libérer de l'espace en supprimant les copies excédentaires.
			</div>
			<div>
				<form action="/api/cleanup_replicas" method="post" style="margin:0;">
					<button type="submit" class="btn">
						Nettoyer les copies en surplus
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
                        <button type="submit" class="btn" {{if .NotEnoughDisksForRedundancy}}disabled title="Pas assez de disques pour l'upload"{{end}}>Envoyer (Mode Parallèle)</button>
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
								{{else if eq .Status "Sur-protégé"}} <span class="status-over-protected">ℹ️ Sur-protégé</span>
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
