package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// --- Constantes et Configuration ---
const (
	volumeSizeGB = 30
)

type Config struct {
	Name    string
	DiskID  string
	Storage string
	Server  string
	Address string
}

type DeletedChunk struct {
	Offset uint64
	Size   uint32
}

type OrphanChunk struct {
	Checksum string
	Offset   uint64
	Size     uint32
	Created  time.Time
}

var (
	diskConfig    Config
	volumePath    string
	volumeMutex   = &sync.Mutex{}
	deletedChunks = make(map[uint64]uint32) // offset -> size
	deletedMutex  = &sync.RWMutex{}
	orphanChunks  = make(map[string]OrphanChunk) // checksum -> chunk info
	orphanMutex   = &sync.RWMutex{}
)

// --- Fonctions Principales ---

func main() {
	name := flag.String("name", "", "Nom unique du volume (requis)")
	diskID := flag.String("disk", "", "Identifiant du disque physique parent (requis)")
	storage := flag.String("storage", ".", "Emplacement de stockage pour le fichier de volume")
	server := flag.String("server", "localhost:8080", "Adresse IP:port du serveur d'index")
	address := flag.String("address", "localhost:9000", "Adresse IP:port de ce volume pour écouter")
	flag.Parse()

	if *name == "" || *diskID == "" {
		log.Fatal("Les arguments -name et -disk sont requis.")
	}

	diskConfig = Config{
		Name:    *name,
		DiskID:  *diskID,
		Storage: *storage,
		Server:  *server,
		Address: *address,
	}

	volumeFileName := fmt.Sprintf("%s.dat", diskConfig.Name)
	volumePath = filepath.Join(diskConfig.Storage, volumeFileName)

	log.Printf("Démarrage du volume '%s' sur le disque physique '%s' (%s)", diskConfig.Name, diskConfig.DiskID, diskConfig.Address)
	log.Printf("Utilisation du fichier de volume : %s", volumePath)

	ensureVolumeFile()
	loadDeletedChunks()
	loadOrphanChunks()

	go cleanupOldOrphans()

	if err := initialRegister(); err != nil {
		log.Fatalf("Impossible de démarrer le volume. Erreur d'enregistrement : %v", err)
	}

	go registerWithServer()

	http.HandleFunc("/write_chunk", writeChunkHandler)
	http.HandleFunc("/read_chunk", readChunkHandler)
	http.HandleFunc("/verify_chunk", verifyChunkHandler)
	http.HandleFunc("/mark_deleted", markDeletedHandler)
	http.HandleFunc("/compact", compactHandler)
	http.HandleFunc("/cleanup_orphan", cleanupOrphanHandler)
	http.HandleFunc("/health", healthCheckHandler)

	log.Printf("Volume '%s' en écoute sur http://%s", diskConfig.Name, diskConfig.Address)
	if err := http.ListenAndServe(diskConfig.Address, nil); err != nil {
		log.Fatalf("Le serveur du volume n'a pas pu démarrer : %v", err)
	}
}

// --- Logique Métier ---

func ensureVolumeFile() {
	if err := os.MkdirAll(diskConfig.Storage, 0755); err != nil {
		log.Fatalf("Impossible de créer le répertoire de stockage : %v", err)
	}
	if _, err := os.Stat(volumePath); os.IsNotExist(err) {
		log.Printf("Création du fichier de volume : %s", volumePath)
		file, err := os.Create(volumePath)
		if err != nil {
			log.Fatalf("Impossible de créer le fichier de volume : %v", err)
		}
		file.Close()
	} else {
		log.Printf("Fichier de volume existant trouvé : %s", volumePath)
	}
}

func loadDeletedChunks() {
	deletedPath := filepath.Join(diskConfig.Storage, fmt.Sprintf("%s.deleted", diskConfig.Name))
	file, err := os.Open(deletedPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Aucun fichier de chunks supprimés trouvé, démarrage avec une liste vide")
			return
		}
		log.Printf("Erreur lors du chargement des chunks supprimés : %v", err)
		return
	}
	defer file.Close()

	var chunks []DeletedChunk
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&chunks); err != nil {
		log.Printf("Erreur lors du décodage des chunks supprimés : %v", err)
		return
	}

	deletedMutex.Lock()
	for _, chunk := range chunks {
		deletedChunks[chunk.Offset] = chunk.Size
	}
	deletedMutex.Unlock()

	log.Printf("Chargé %d chunks marqués pour suppression", len(chunks))
}

func loadOrphanChunks() {
	orphanPath := filepath.Join(diskConfig.Storage, fmt.Sprintf("%s.orphans", diskConfig.Name))
	file, err := os.Open(orphanPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Aucun fichier de chunks orphelins trouvé")
			return
		}
		log.Printf("Erreur lors du chargement des chunks orphelins : %v", err)
		return
	}
	defer file.Close()

	var orphans []OrphanChunk
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&orphans); err != nil {
		log.Printf("Erreur lors du décodage des chunks orphelins : %v", err)
		return
	}

	orphanMutex.Lock()
	for _, orphan := range orphans {
		orphanChunks[orphan.Checksum] = orphan
	}
	orphanMutex.Unlock()

	log.Printf("Chargé %d chunks orphelins", len(orphans))
}

func saveDeletedChunks() {
	deletedPath := filepath.Join(diskConfig.Storage, fmt.Sprintf("%s.deleted", diskConfig.Name))

	deletedMutex.RLock()
	var chunks []DeletedChunk
	for offset, size := range deletedChunks {
		chunks = append(chunks, DeletedChunk{Offset: offset, Size: size})
	}
	deletedMutex.RUnlock()

	tempPath := deletedPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		log.Printf("Erreur lors de la création du fichier temporaire pour les chunks supprimés : %v", err)
		return
	}

	encoder := json.NewEncoder(file)
	err = encoder.Encode(chunks)
	file.Close()

	if err != nil {
		log.Printf("Erreur lors de l'encodage des chunks supprimés : %v", err)
		os.Remove(tempPath)
		return
	}

	if err := os.Rename(tempPath, deletedPath); err != nil {
		log.Printf("Erreur lors de la finalisation de la sauvegarde des chunks supprimés : %v", err)
		os.Remove(tempPath)
		return
	}
}

func saveOrphanChunks() {
	orphanPath := filepath.Join(diskConfig.Storage, fmt.Sprintf("%s.orphans", diskConfig.Name))

	orphanMutex.RLock()
	var orphans []OrphanChunk
	for _, orphan := range orphanChunks {
		orphans = append(orphans, orphan)
	}
	orphanMutex.RUnlock()

	tempPath := orphanPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		log.Printf("Erreur lors de la création du fichier temporaire pour les chunks orphelins : %v", err)
		return
	}

	encoder := json.NewEncoder(file)
	err = encoder.Encode(orphans)
	file.Close()

	if err != nil {
		log.Printf("Erreur lors de l'encodage des chunks orphelins : %v", err)
		os.Remove(tempPath)
		return
	}

	if err := os.Rename(tempPath, orphanPath); err != nil {
		log.Printf("Erreur lors de la finalisation de la sauvegarde des chunks orphelins : %v", err)
		os.Remove(tempPath)
		return
	}
}

func cleanupOldOrphans() {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		orphanMutex.Lock()
		cutoff := time.Now().Add(-24 * time.Hour)
		cleaned := 0

		for checksum, orphan := range orphanChunks {
			if orphan.Created.Before(cutoff) {
				delete(orphanChunks, checksum)
				cleaned++
				log.Printf("Chunk orphelin ancien supprimé après 24h : %s", checksum[:8])
			}
		}
		orphanMutex.Unlock()

		if cleaned > 0 {
			saveOrphanChunks()
			log.Printf("Nettoyage automatique : %d chunks orphelins anciens (>24h) supprimés", cleaned)
		}
	}
}

func checkSpaceAvailable(requiredBytes uint64) error {
	freeSpace := getFreeSpaceBytes()
	if freeSpace < requiredBytes {
		return fmt.Errorf("espace insuffisant : %d bytes requis, %d bytes disponibles", requiredBytes, freeSpace)
	}
	return nil
}

func getFreeSpaceBytes() uint64 {
	totalBytes := uint64(volumeSizeGB) * 1024 * 1024 * 1024

	fileInfo, err := os.Stat(volumePath)
	if err != nil {
		return totalBytes
	}

	usedBytes := uint64(fileInfo.Size())

	deletedMutex.RLock()
	var deletedSize uint64
	for _, size := range deletedChunks {
		deletedSize += uint64(size)
	}
	deletedMutex.RUnlock()

	effectiveUsedBytes := usedBytes
	if deletedSize < usedBytes {
		effectiveUsedBytes = usedBytes - deletedSize
	}

	if effectiveUsedBytes >= totalBytes {
		return 0
	}
	return totalBytes - effectiveUsedBytes
}

func buildStatusPayload(requestType string) ([]byte, error) {
	status := map[string]interface{}{
		"type":       requestType,
		"name":       diskConfig.Name,
		"diskId":     diskConfig.DiskID,
		"address":    diskConfig.Address,
		"totalSpace": uint64(volumeSizeGB) * 1024 * 1024 * 1024,
		"freeSpace":  getFreeSpaceBytes(),
	}
	return json.Marshal(status)
}

func initialRegister() error {
	payload, err := buildStatusPayload("initial")
	if err != nil {
		return fmt.Errorf("impossible de construire la charge utile JSON : %v", err)
	}

	serverURL := fmt.Sprintf("http://%s/api/disk/register", diskConfig.Server)
	resp, err := http.Post(serverURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("erreur de connexion au serveur %s : %v", diskConfig.Server, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Println("Enregistrement initial auprès du serveur réussi.")
		return nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("conflit de nom : %s", string(bodyBytes))
	}

	return fmt.Errorf("le serveur a répondu avec un statut inattendu %s. Réponse : %s", resp.Status, string(bodyBytes))
}

func registerWithServer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		payload, err := buildStatusPayload("heartbeat")
		if err != nil {
			log.Printf("Erreur lors de la création du payload pour le heartbeat : %v", err)
			continue
		}

		serverURL := fmt.Sprintf("http://%s/api/disk/register", diskConfig.Server)
		resp, err := http.Post(serverURL, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("Erreur de connexion au serveur %s : %v", diskConfig.Server, err)
		} else {
			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("Le serveur a répondu avec un statut non-OK lors du heartbeat : %s. Réponse : %s", resp.Status, string(bodyBytes))
			}
			resp.Body.Close()
		}
	}
}

// CORRECTION: Fonction de compactage entièrement réécrite pour retourner la map des offsets
func compactVolume() (map[uint64]uint64, error) {
	log.Printf("Début du compactage du volume '%s'", diskConfig.Name)

	deletedMutex.RLock()
	if len(deletedChunks) == 0 {
		deletedMutex.RUnlock()
		log.Printf("Aucun chunk à supprimer, compactage ignoré.")
		return nil, nil
	}

	type deletedRegion struct {
		offset uint64
		size   uint32
	}
	var deletedRegions []deletedRegion
	for offset, size := range deletedChunks {
		deletedRegions = append(deletedRegions, deletedRegion{offset, size})
	}
	deletedMutex.RUnlock()

	sort.Slice(deletedRegions, func(i, j int) bool {
		return deletedRegions[i].offset < deletedRegions[j].offset
	})

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	originalFile, err := os.Open(volumePath)
	if err != nil {
		return nil, fmt.Errorf("impossible d'ouvrir le fichier original: %w", err)
	}
	defer originalFile.Close()

	fileInfo, err := originalFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("impossible de lire les métadonnées du fichier original: %w", err)
	}
	originalSize := uint64(fileInfo.Size())

	tempPath := volumePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("impossible de créer le fichier temporaire: %w", err)
	}
	defer tempFile.Close()

	offsetMapping := make(map[uint64]uint64)
	var currentReadOffset uint64 = 0
	var currentWriteOffset uint64 = 0

	for _, region := range deletedRegions {
		validBlockSize := region.offset - currentReadOffset
		if validBlockSize > 0 {
			// Enregistrer le mapping pour le début de ce bloc valide
			offsetMapping[currentReadOffset] = currentWriteOffset

			// Copier le bloc de données valide
			bytesCopied, err := io.CopyN(tempFile, io.NewSectionReader(originalFile, int64(currentReadOffset), int64(validBlockSize)), int64(validBlockSize))
			if err != nil {
				os.Remove(tempPath)
				return nil, fmt.Errorf("erreur de copie du bloc valide: %w", err)
			}
			currentWriteOffset += uint64(bytesCopied)
		}
		// Sauter par-dessus la région supprimée
		currentReadOffset = region.offset + uint64(region.size)
	}

	// Copier le dernier bloc de données valide (s'il existe) après la dernière région supprimée
	if currentReadOffset < originalSize {
		offsetMapping[currentReadOffset] = currentWriteOffset
		bytesCopied, err := io.Copy(tempFile, io.NewSectionReader(originalFile, int64(currentReadOffset), int64(originalSize-currentReadOffset)))
		if err != nil {
			os.Remove(tempPath)
			return nil, fmt.Errorf("erreur de copie du dernier bloc: %w", err)
		}
		currentWriteOffset += uint64(bytesCopied)
	}
	
	originalFile.Close()
	tempFile.Close()

	if err := os.Rename(tempPath, volumePath); err != nil {
		os.Remove(tempPath)
		return nil, fmt.Errorf("impossible de remplacer le fichier original: %w", err)
	}

	deletedMutex.Lock()
	deletedCount := len(deletedChunks)
	deletedChunks = make(map[uint64]uint32)
	deletedMutex.Unlock()
	saveDeletedChunks()

	log.Printf("Compactage terminé : %d chunks supprimés, fichier optimisé. %d mappings d'offset créés.", deletedCount, len(offsetMapping))
	return offsetMapping, nil
}


// --- Handlers HTTP ---

func verifyChunkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
		return
	}

	offsetStr := r.URL.Query().Get("offset")
	sizeStr := r.URL.Query().Get("size")
	expectedChecksum := r.URL.Query().Get("checksum")

	if offsetStr == "" || sizeStr == "" || expectedChecksum == "" {
		http.Error(w, "Paramètres 'offset', 'size' et 'checksum' requis", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "Paramètre 'offset' invalide", http.StatusBadRequest)
		return
	}

	size, err := strconv.ParseUint(sizeStr, 10, 32)
	if err != nil {
		http.Error(w, "Paramètre 'size' invalide", http.StatusBadRequest)
		return
	}

	deletedMutex.RLock()
	if _, isDeleted := deletedChunks[offset]; isDeleted {
		deletedMutex.RUnlock()
		http.Error(w, "Chunk marqué comme supprimé", http.StatusNotFound)
		return
	}
	deletedMutex.RUnlock()

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	file, err := os.Open(volumePath)
	if err != nil {
		http.Error(w, "Erreur d'accès au volume", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		http.Error(w, "Erreur de lecture des métadonnées du volume", http.StatusInternalServerError)
		return
	}

	if int64(offset+uint64(size)) > fileInfo.Size() {
		http.Error(w, "Chunk au-delà de la fin du volume", http.StatusNotFound)
		return
	}

	chunkData := make([]byte, size)
	_, err = file.ReadAt(chunkData, int64(offset))
	if err != nil {
		http.Error(w, "Erreur de lecture du chunk", http.StatusInternalServerError)
		return
	}

	hash := sha256.Sum256(chunkData)
	actualChecksum := hex.EncodeToString(hash[:])

	if actualChecksum != expectedChecksum {
		log.Printf("ERREUR VERIFICATION: Chunk corrompu détecté - offset:%d, size:%d, attendu:%s, réel:%s",
			offset, size, expectedChecksum, actualChecksum)
		http.Error(w, "Chunk corrompu", http.StatusConflict)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":   "ok",
		"checksum": actualChecksum,
		"size":     size,
		"offset":   offset,
	}
	json.NewEncoder(w).Encode(response)
}

func writeChunkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
		return
	}

	expectedChecksum := r.Header.Get("X-Chunk-Checksum")
	if expectedChecksum == "" {
		http.Error(w, "Checksum manquant dans l'en-tête X-Chunk-Checksum", http.StatusBadRequest)
		return
	}

	chunkData, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Erreur lors de la lecture du chunk", http.StatusInternalServerError)
		return
	}

	if err := checkSpaceAvailable(uint64(len(chunkData)) + 10*1024*1024); err != nil {
		log.Printf("ERREUR ESPACE: %v", err)
		http.Error(w, "Espace disque insuffisant", http.StatusInsufficientStorage)
		return
	}

	hash := sha256.Sum256(chunkData)
	actualChecksum := hex.EncodeToString(hash[:])

	if actualChecksum != expectedChecksum {
		log.Printf("ERREUR: Checksum invalide pour l'écriture. Attendu: %s, Reçu: %s", expectedChecksum, actualChecksum)
		http.Error(w, "Checksum invalide. Les données ont pu être corrompues pendant le transfert.", http.StatusBadRequest)
		return
	}

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	file, err := os.OpenFile(volumePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("ERREUR CRITIQUE: Impossible d'ouvrir le fichier de volume '%s': %v", volumePath, err)
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Printf("ERREUR CRITIQUE: Impossible de se positionner à la fin du volume: %v", err)
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}

	bytesWritten, err := file.Write(chunkData)
	if err != nil {
		log.Printf("ERREUR CRITIQUE: Échec d'écriture sur le volume à l'offset %d: %v", offset, err)
		orphanMutex.Lock()
		orphanChunks[actualChecksum] = OrphanChunk{
			Checksum: actualChecksum,
			Offset:   uint64(offset),
			Size:     uint32(len(chunkData)),
			Created:  time.Now(),
		}
		orphanMutex.Unlock()
		saveOrphanChunks()
		http.Error(w, "Erreur lors de l'écriture du chunk", http.StatusInternalServerError)
		return
	}

	if bytesWritten != len(chunkData) {
		log.Printf("ERREUR CRITIQUE: Écriture incomplète - attendu: %d bytes, écrit: %d bytes",
			len(chunkData), bytesWritten)
		orphanMutex.Lock()
		orphanChunks[actualChecksum] = OrphanChunk{
			Checksum: actualChecksum,
			Offset:   uint64(offset),
			Size:     uint32(bytesWritten),
			Created:  time.Now(),
		}
		orphanMutex.Unlock()
		saveOrphanChunks()
		http.Error(w, "Écriture incomplète du chunk", http.StatusInternalServerError)
		return
	}

	if err := file.Sync(); err != nil {
		log.Printf("ERREUR CRITIQUE: Impossible de synchroniser les données sur disque: %v", err)
	}

	file.Close()

	readFile, err := os.Open(volumePath)
	if err != nil {
		log.Printf("ERREUR VERIFICATION: Impossible de rouvrir le volume pour vérification: %v", err)
	} else {
		verifyData := make([]byte, bytesWritten)
		_, err = readFile.ReadAt(verifyData, offset)
		readFile.Close()

		if err != nil {
			log.Printf("ERREUR VERIFICATION: Impossible de relire le chunk écrit: %v", err)
		} else {
			verifyHash := sha256.Sum256(verifyData)
			verifyChecksum := hex.EncodeToString(verifyHash[:])

			if verifyChecksum != expectedChecksum {
				log.Printf("ERREUR CRITIQUE: Corruption détectée immédiatement après écriture!")
				log.Printf("Checksum attendu: %s, checksum lu: %s", expectedChecksum, verifyChecksum)
				orphanMutex.Lock()
				orphanChunks[actualChecksum] = OrphanChunk{
					Checksum: actualChecksum,
					Offset:   uint64(offset),
					Size:     uint32(bytesWritten),
					Created:  time.Now(),
				}
				orphanMutex.Unlock()
				saveOrphanChunks()
				http.Error(w, "Corruption détectée après écriture", http.StatusInternalServerError)
				return
			}
		}
	}

	response := map[string]interface{}{"offset": offset, "size": uint32(bytesWritten)}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
	log.Printf("Chunk écrit et vérifié (checksum OK: %s...): taille %d, offset %d", actualChecksum[:8], bytesWritten, offset)
}

func readChunkHandler(w http.ResponseWriter, r *http.Request) {
	var offset, size int64
	_, errO := fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)
	_, errS := fmt.Sscanf(r.URL.Query().Get("size"), "%d", &size)
	if errO != nil || errS != nil || size <= 0 {
		http.Error(w, "Paramètres 'offset' et 'size' invalides", http.StatusBadRequest)
		return
	}

	deletedMutex.RLock()
	if _, isDeleted := deletedChunks[uint64(offset)]; isDeleted {
		deletedMutex.RUnlock()
		http.Error(w, "Chunk supprimé", http.StatusNotFound)
		return
	}
	deletedMutex.RUnlock()

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	file, err := os.Open(volumePath)
	if err != nil {
		log.Printf("ERREUR LECTURE: Impossible d'ouvrir le volume: %v", err)
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Printf("ERREUR LECTURE: Impossible de lire les métadonnées du volume: %v", err)
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}

	if offset+size > fileInfo.Size() {
		log.Printf("ERREUR LECTURE: Tentative de lecture au-delà de la fin du fichier - offset:%d, size:%d, fichier:%d",
			offset, size, fileInfo.Size())
		http.Error(w, "Offset de lecture invalide - au-delà de la fin du fichier", http.StatusBadRequest)
		return
	}

	chunkData := make([]byte, size)
	bytesRead, err := file.ReadAt(chunkData, offset)
	if err != nil {
		log.Printf("ERREUR LECTURE: Échec de lecture à l'offset %d: %v", offset, err)
		http.Error(w, "Erreur de lecture du chunk", http.StatusInternalServerError)
		return
	}

	if int64(bytesRead) != size {
		log.Printf("ERREUR LECTURE: Lecture incomplète - attendu:%d, lu:%d", size, bytesRead)
		http.Error(w, "Lecture incomplète du chunk", http.StatusInternalServerError)
		return
	}

	hash := sha256.Sum256(chunkData)
	actualChecksum := hex.EncodeToString(hash[:])
	w.Header().Set("X-Chunk-Checksum", actualChecksum)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))

	bytesWritten, err := w.Write(chunkData)
	if err != nil {
		log.Printf("ERREUR LECTURE: Échec d'envoi des données au client: %v", err)
		return
	}

	if bytesWritten != len(chunkData) {
		log.Printf("ERREUR LECTURE: Envoi incomplet au client - envoyé:%d, total:%d",
			bytesWritten, len(chunkData))
		return
	}

	log.Printf("Chunk lu avec succès (checksum: %s...): taille %d, offset %d", actualChecksum[:8], size, offset)
}

func markDeletedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
		return
	}

	var deleteData struct {
		Chunks []struct {
			Offset uint64 `json:"offset"`
			Size   uint32 `json:"size"`
		} `json:"chunks"`
	}

	if err := json.NewDecoder(r.Body).Decode(&deleteData); err != nil {
		http.Error(w, "JSON invalide: "+err.Error(), http.StatusBadRequest)
		return
	}

	deletedMutex.Lock()
	chunksMarked := 0
	for _, chunk := range deleteData.Chunks {
		if _, exists := deletedChunks[chunk.Offset]; !exists {
			deletedChunks[chunk.Offset] = chunk.Size
			chunksMarked++
		}
	}
	deletedMutex.Unlock()

	if chunksMarked > 0 {
		saveDeletedChunks()
		log.Printf("%d chunks marqués pour suppression", chunksMarked)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"marked": chunksMarked,
		"total":  len(deleteData.Chunks),
	})
}

func cleanupOrphanHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
		return
	}

	var cleanupData struct {
		Chunks []struct {
			Checksum string `json:"checksum"`
			Offset   string `json:"offset"`
			Size     string `json:"size"`
		} `json:"chunks,omitempty"`
		Checksum string `json:"checksum,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&cleanupData); err != nil {
		http.Error(w, "JSON invalide: "+err.Error(), http.StatusBadRequest)
		return
	}

	orphanMutex.Lock()
	cleanedCount := 0

	if cleanupData.Checksum != "" {
		if _, exists := orphanChunks[cleanupData.Checksum]; exists {
			delete(orphanChunks, cleanupData.Checksum)
			cleanedCount++
		}
	}

	for _, chunk := range cleanupData.Chunks {
		if chunk.Checksum != "" {
			if _, exists := orphanChunks[chunk.Checksum]; exists {
				delete(orphanChunks, chunk.Checksum)
				cleanedCount++
			}
		} else if chunk.Offset != "" && chunk.Size != "" {
			offset, _ := strconv.ParseUint(chunk.Offset, 10, 64)
			size, _ := strconv.ParseUint(chunk.Size, 10, 32)

			for checksum, orphan := range orphanChunks {
				if orphan.Offset == offset && orphan.Size == uint32(size) {
					delete(orphanChunks, checksum)
					cleanedCount++
					break
				}
			}
		}
	}
	orphanMutex.Unlock()

	if cleanedCount > 0 {
		saveOrphanChunks()
		log.Printf("Nettoyage d'orphelins : %d chunks supprimés", cleanedCount)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"cleaned": cleanedCount,
	})
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
		return
	}

	health := map[string]interface{}{
		"status":    "online",
		"volume":    diskConfig.Name,
		"diskId":    diskConfig.DiskID,
		"freeSpace": getFreeSpaceBytes(),
		"timestamp": time.Now().Unix(),
	}

	issues := make([]string, 0)

	if _, err := os.Stat(volumePath); err != nil {
		issues = append(issues, "volume file inaccessible")
		health["status"] = "degraded"
	}

	freeSpace := getFreeSpaceBytes()
	if freeSpace < 100*1024*1024 {
		issues = append(issues, "low disk space")
		health["status"] = "warning"
	}

	orphanMutex.RLock()
	orphanCount := len(orphanChunks)
	orphanMutex.RUnlock()

	if orphanCount > 100 {
		issues = append(issues, "too many orphan chunks")
		health["status"] = "warning"
	}

	deletedMutex.RLock()
	deletedCount := len(deletedChunks)
	deletedMutex.RUnlock()

	if deletedCount > 1000 {
		issues = append(issues, "too many deleted chunks pending cleanup")
		health["status"] = "warning"
	}

	health["orphan_chunks"] = orphanCount
	health["deleted_chunks_pending"] = deletedCount

	if len(issues) > 0 {
		health["issues"] = issues
	}

	switch health["status"] {
	case "degraded":
		w.WriteHeader(http.StatusServiceUnavailable)
	case "warning":
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// CORRECTION: Le handler de compactage renvoie maintenant la map des offsets
func compactHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
		return
	}

	offsetMap, err := compactVolume()
	if err != nil {
		log.Printf("Erreur lors du compactage : %v", err)
		http.Error(w, "Erreur interne lors du compactage", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "Compactage terminé",
		"offset_map": offsetMap,
	})
}
