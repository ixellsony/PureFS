// --- volume/volume.go ---

package volume

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

const (
	volumeSizeGB = 30
)

// Config contient tous les paramètres nécessaires pour créer une instance de Volume.
type Config struct {
	Name       string
	DiskID     string
	StorageDir string // Répertoire où le .dat et les métadonnées seront stockés
	ServerAddr string // Adresse du serveur principal (ex: "localhost:8080")
	ListenAddr string // Adresse de l'AGENT qui écoute (ex: "0.0.0.0:9000")
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

type ChunkToKeep struct {
	Offset uint64 `json:"offset"`
	Size   uint32 `json:"size"`
}

type CompactionInstruction struct {
	ChunksToKeep []ChunkToKeep `json:"chunks_to_keep"`
}

// Volume encapsule l'état et la logique d'un seul volume de 30 Go.
type Volume struct {
	config        Config
	volumePath    string
	volumeFile    *os.File
	volumeMutex   *sync.Mutex
	deletedChunks map[uint64]uint32
	deletedMutex  *sync.RWMutex
	orphanChunks  map[string]OrphanChunk
	orphanMutex   *sync.RWMutex
}

// New crée et initialise une nouvelle instance de Volume.
func New(cfg Config) (*Volume, error) {
	v := &Volume{
		config:        cfg,
		volumeMutex:   &sync.Mutex{},
		deletedChunks: make(map[uint64]uint32),
		deletedMutex:  &sync.RWMutex{},
		orphanChunks:  make(map[string]OrphanChunk),
		orphanMutex:   &sync.RWMutex{},
	}

	volumeFileName := fmt.Sprintf("%s.dat", v.config.Name)
	v.volumePath = filepath.Join(v.config.StorageDir, volumeFileName)

	log.Printf("Initialisation du volume '%s' sur le disque '%s'", v.config.Name, v.config.DiskID)

	if err := v.ensureVolumeFile(); err != nil {
		return nil, fmt.Errorf("erreur lors de la création du fichier pour le volume %s: %w", v.config.Name, err)
	}

	var err error
	v.volumeFile, err = os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("impossible d'ouvrir le fichier de volume '%s': %w", v.config.Name, err)
	}

	v.loadDeletedChunks()
	v.loadOrphanChunks()

	return v, nil
}

// Start lance les tâches de fond du volume (heartbeats). C'est une méthode non-bloquante.
func (v *Volume) Start() error {
	log.Printf("Démarrage de la logique du volume '%s'...", v.config.Name)

	if err := v.initialRegister(); err != nil {
		v.volumeFile.Close() // Nettoyage en cas d'échec
		return fmt.Errorf("impossible de démarrer le volume '%s'. Erreur d'enregistrement : %w", v.config.Name, err)
	}

	go v.cleanupOldOrphans()
	go v.registerWithServer() // Tâche de fond pour les heartbeats

	log.Printf("Volume '%s' est prêt et envoie des heartbeats.", v.config.Name)
	return nil
}

// --- Logique Métier (interne au volume) ---

func (v *Volume) ensureVolumeFile() error {
	if _, err := os.Stat(v.volumePath); os.IsNotExist(err) {
		log.Printf("Création du fichier de volume : %s", v.volumePath)
		file, err := os.Create(v.volumePath)
		if err != nil {
			return fmt.Errorf("impossible de créer le fichier de volume : %w", err)
		}
		return file.Close()
	}
	log.Printf("Fichier de volume existant trouvé : %s", v.volumePath)
	return nil
}

func (v *Volume) loadDeletedChunks() {
	deletedPath := filepath.Join(v.config.StorageDir, fmt.Sprintf("%s.deleted", v.config.Name))
	file, err := os.Open(deletedPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("Erreur lors du chargement des chunks supprimés pour le volume %s : %v", v.config.Name, err)
		return
	}
	defer file.Close()

	var chunks []DeletedChunk
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&chunks); err != nil {
		log.Printf("Erreur lors du décodage des chunks supprimés pour le volume %s : %v", v.config.Name, err)
		return
	}

	v.deletedMutex.Lock()
	for _, chunk := range chunks {
		v.deletedChunks[chunk.Offset] = chunk.Size
	}
	v.deletedMutex.Unlock()
}

func (v *Volume) loadOrphanChunks() {
	orphanPath := filepath.Join(v.config.StorageDir, fmt.Sprintf("%s.orphans", v.config.Name))
	file, err := os.Open(orphanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Printf("Erreur lors du chargement des chunks orphelins pour le volume %s : %v", v.config.Name, err)
		return
	}
	defer file.Close()

	var orphans []OrphanChunk
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&orphans); err != nil {
		log.Printf("Erreur lors du décodage des chunks orphelins pour le volume %s: %v", v.config.Name, err)
		return
	}

	v.orphanMutex.Lock()
	for _, orphan := range orphans {
		v.orphanChunks[orphan.Checksum] = orphan
	}
	v.orphanMutex.Unlock()
}

func (v *Volume) saveDeletedChunks() {
	deletedPath := filepath.Join(v.config.StorageDir, fmt.Sprintf("%s.deleted", v.config.Name))

	v.deletedMutex.RLock()
	var chunks []DeletedChunk
	for offset, size := range v.deletedChunks {
		chunks = append(chunks, DeletedChunk{Offset: offset, Size: size})
	}
	v.deletedMutex.RUnlock()

	tempPath := deletedPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		log.Printf("Erreur (vol %s): création fichier temp pour chunks supprimés: %v", v.config.Name, err)
		return
	}

	encoder := json.NewEncoder(file)
	err = encoder.Encode(chunks)
	file.Close()

	if err != nil {
		log.Printf("Erreur (vol %s): encodage chunks supprimés: %v", v.config.Name, err)
		os.Remove(tempPath)
		return
	}

	if err := os.Rename(tempPath, deletedPath); err != nil {
		log.Printf("Erreur (vol %s): finalisation sauvegarde chunks supprimés: %v", v.config.Name, err)
		os.Remove(tempPath)
	}
}

func (v *Volume) saveOrphanChunks() {
	orphanPath := filepath.Join(v.config.StorageDir, fmt.Sprintf("%s.orphans", v.config.Name))

	v.orphanMutex.RLock()
	var orphans []OrphanChunk
	for _, orphan := range v.orphanChunks {
		orphans = append(orphans, orphan)
	}
	v.orphanMutex.RUnlock()

	tempPath := orphanPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		log.Printf("Erreur (vol %s): création fichier temp pour chunks orphelins: %v", v.config.Name, err)
		return
	}

	encoder := json.NewEncoder(file)
	err = encoder.Encode(orphans)
	file.Close()

	if err != nil {
		log.Printf("Erreur (vol %s): encodage chunks orphelins: %v", v.config.Name, err)
		os.Remove(tempPath)
		return
	}

	if err := os.Rename(tempPath, orphanPath); err != nil {
		log.Printf("Erreur (vol %s): finalisation sauvegarde chunks orphelins: %v", v.config.Name, err)
		os.Remove(tempPath)
	}
}

func (v *Volume) cleanupOldOrphans() {
	ticker := time.NewTicker(6 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		v.orphanMutex.Lock()
		cutoff := time.Now().Add(-24 * time.Hour)
		cleaned := 0

		for checksum, orphan := range v.orphanChunks {
			if orphan.Created.Before(cutoff) {
				delete(v.orphanChunks, checksum)
				cleaned++
			}
		}
		v.orphanMutex.Unlock()

		if cleaned > 0 {
			v.saveOrphanChunks()
			log.Printf("Nettoyage (vol %s): %d chunks orphelins anciens (>24h) supprimés", v.config.Name, cleaned)
		}
	}
}

func (v *Volume) getFreeSpaceBytes() uint64 {
	totalBytes := uint64(volumeSizeGB) * 1024 * 1024 * 1024

	fileInfo, err := v.volumeFile.Stat()
	if err != nil {
		return totalBytes // Si le fichier n'a pas de taille, on considère tout l'espace comme libre
	}

	usedBytes := uint64(fileInfo.Size())

	v.deletedMutex.RLock()
	var deletedSize uint64
	for _, size := range v.deletedChunks {
		deletedSize += uint64(size)
	}
	v.deletedMutex.RUnlock()

	effectiveUsedBytes := usedBytes
	if deletedSize < usedBytes {
		effectiveUsedBytes = usedBytes - deletedSize
	}

	if effectiveUsedBytes >= totalBytes {
		return 0
	}
	return totalBytes - effectiveUsedBytes
}

func (v *Volume) buildStatusPayload(requestType string) ([]byte, error) {
	status := map[string]interface{}{
		"type":       requestType,
		"name":       v.config.Name,
		"diskId":     v.config.DiskID,
		"address":    v.config.ListenAddr, // Utilise l'adresse de l'agent
		"totalSpace": uint64(volumeSizeGB) * 1024 * 1024 * 1024,
		"freeSpace":  v.getFreeSpaceBytes(),
	}
	return json.Marshal(status)
}

func (v *Volume) initialRegister() error {
	payload, err := v.buildStatusPayload("initial")
	if err != nil {
		return fmt.Errorf("impossible de construire la charge utile JSON : %w", err)
	}

	serverURL := fmt.Sprintf("http://%s/api/disk/register", v.config.ServerAddr)
	resp, err := http.Post(serverURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("erreur de connexion au serveur %s : %w", v.config.ServerAddr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("Enregistrement initial du volume '%s' réussi.", v.config.Name)
		return nil
	}

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("conflit de nom : %s", string(bodyBytes))
	}

	return fmt.Errorf("le serveur a répondu avec un statut inattendu %s. Réponse : %s", resp.Status, string(bodyBytes))
}

func (v *Volume) registerWithServer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		payload, err := v.buildStatusPayload("heartbeat")
		if err != nil {
			log.Printf("Erreur (vol %s): création du payload heartbeat : %v", v.config.Name, err)
			continue
		}

		serverURL := fmt.Sprintf("http://%s/api/disk/register", v.config.ServerAddr)
		resp, err := http.Post(serverURL, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("Erreur (vol %s): connexion au serveur %s : %v", v.config.Name, v.config.ServerAddr, err)
		} else {
			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("Le serveur a répondu non-OK au heartbeat du vol %s: %s. Réponse : %s", v.config.Name, resp.Status, string(bodyBytes))
			}
			resp.Body.Close()
		}
	}
}

// --- Handlers (maintenant des méthodes publiques appelées par l'agent) ---

func (v *Volume) WriteChunkHandler(w http.ResponseWriter, r *http.Request) {
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

	if v.getFreeSpaceBytes() < uint64(len(chunkData)) {
		log.Printf("ERREUR ESPACE (vol %s): Espace insuffisant", v.config.Name)
		http.Error(w, "Espace disque insuffisant", http.StatusInsufficientStorage)
		return
	}

	hash := sha256.Sum256(chunkData)
	actualChecksum := hex.EncodeToString(hash[:])

	if actualChecksum != expectedChecksum {
		http.Error(w, "Checksum invalide.", http.StatusBadRequest)
		return
	}

	v.volumeMutex.Lock()
	defer v.volumeMutex.Unlock()

	offset, err := v.volumeFile.Seek(0, io.SeekEnd)
	if err != nil {
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}

	bytesWritten, err := v.volumeFile.Write(chunkData)
	if err != nil {
		v.orphanMutex.Lock()
		v.orphanChunks[actualChecksum] = OrphanChunk{
			Checksum: actualChecksum, Offset: uint64(offset), Size: uint32(len(chunkData)), Created: time.Now(),
		}
		v.orphanMutex.Unlock()
		v.saveOrphanChunks()
		http.Error(w, "Erreur lors de l'écriture du chunk", http.StatusInternalServerError)
		return
	}

	if err := v.volumeFile.Sync(); err != nil {
		log.Printf("ERREUR CRITIQUE (vol %s): Impossible de synchroniser les données sur disque: %v", v.config.Name, err)
	}

	response := map[string]interface{}{"offset": offset, "size": uint32(bytesWritten)}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (v *Volume) ReadChunkHandler(w http.ResponseWriter, r *http.Request) {
	offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	size, _ := strconv.ParseInt(r.URL.Query().Get("size"), 10, 64)
	if size <= 0 {
		http.Error(w, "Paramètres 'offset' et 'size' invalides", http.StatusBadRequest)
		return
	}

	v.deletedMutex.RLock()
	if _, isDeleted := v.deletedChunks[uint64(offset)]; isDeleted {
		v.deletedMutex.RUnlock()
		http.Error(w, "Chunk supprimé", http.StatusNotFound)
		return
	}
	v.deletedMutex.RUnlock()

	chunkData := make([]byte, size)

	v.volumeMutex.Lock()
	bytesRead, err := v.volumeFile.ReadAt(chunkData, offset)
	v.volumeMutex.Unlock()

	if err != nil {
		http.Error(w, "Erreur de lecture du chunk", http.StatusInternalServerError)
		return
	}
	if int64(bytesRead) != size {
		http.Error(w, "Lecture incomplète du chunk", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	w.Write(chunkData)
}

func (v *Volume) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "online",
		"volume":    v.config.Name,
		"diskId":    v.config.DiskID,
		"freeSpace": v.getFreeSpaceBytes(),
		"timestamp": time.Now().Unix(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

func (v *Volume) CompactHandler(w http.ResponseWriter, r *http.Request) {
	var instruction CompactionInstruction
	if err := json.NewDecoder(r.Body).Decode(&instruction); err != nil {
		http.Error(w, "Corps de requête JSON invalide: "+err.Error(), http.StatusBadRequest)
		return
	}

	offsetMap, err := v.compactVolume(instruction.ChunksToKeep)
	if err != nil {
		log.Printf("Erreur lors du compactage (vol %s) : %v", v.config.Name, err)
		http.Error(w, "Erreur interne lors du compactage", http.StatusInternalServerError)
		return
	}

	stringOffsetMap := make(map[string]uint64)
	for k, val := range offsetMap {
		stringOffsetMap[strconv.FormatUint(k, 10)] = val
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "Compactage terminé",
		"offset_map": stringOffsetMap,
	})
}

func (v *Volume) compactVolume(chunksToKeep []ChunkToKeep) (map[uint64]uint64, error) {
	log.Printf("Début du compactage pour le volume '%s'", v.config.Name)

	sort.Slice(chunksToKeep, func(i, j int) bool {
		return chunksToKeep[i].Offset < chunksToKeep[j].Offset
	})

	v.volumeMutex.Lock()
	defer v.volumeMutex.Unlock()

	if err := v.volumeFile.Close(); err != nil {
		v.volumeFile, _ = os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
		return nil, fmt.Errorf("impossible de fermer le descripteur de fichier existant: %w", err)
	}

	originalFile, err := os.Open(v.volumePath)
	if err != nil {
		v.volumeFile, _ = os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
		return nil, fmt.Errorf("impossible d'ouvrir le fichier original: %w", err)
	}
	defer originalFile.Close()

	tempPath := v.volumePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		v.volumeFile, _ = os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
		return nil, fmt.Errorf("impossible de créer le fichier temporaire: %w", err)
	}
	defer tempFile.Close()

	offsetMapping := make(map[uint64]uint64)
	var currentWriteOffset uint64 = 0

	for _, chunk := range chunksToKeep {
		offsetMapping[chunk.Offset] = currentWriteOffset
		bytesCopied, err := io.CopyN(tempFile, io.NewSectionReader(originalFile, int64(chunk.Offset), int64(chunk.Size)), int64(chunk.Size))
		if err != nil {
			os.Remove(tempPath)
			v.volumeFile, _ = os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
			return nil, fmt.Errorf("erreur de copie du chunk: %w", err)
		}
		currentWriteOffset += uint64(bytesCopied)
	}

	if err := os.Rename(tempPath, v.volumePath); err != nil {
		os.Remove(tempPath)
		v.volumeFile, _ = os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
		return nil, fmt.Errorf("impossible de remplacer le fichier original: %w", err)
	}

	v.volumeFile, err = os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("ERREUR CRITIQUE (vol %s): Fichier de volume remplacé mais impossible de le rouvrir : %v", v.config.Name, err)
	}

	v.deletedMutex.Lock()
	v.deletedChunks = make(map[uint64]uint32)
	v.deletedMutex.Unlock()
	v.saveDeletedChunks()

	log.Printf("Compactage du volume '%s' terminé.", v.config.Name)
	return offsetMapping, nil
}

func (v *Volume) MarkDeletedHandler(w http.ResponseWriter, r *http.Request) {
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
	v.deletedMutex.Lock()
	for _, chunk := range deleteData.Chunks {
		v.deletedChunks[chunk.Offset] = chunk.Size
	}
	v.deletedMutex.Unlock()
	v.saveDeletedChunks()
	w.WriteHeader(http.StatusOK)
}

func (v *Volume) VerifyChunkHandler(w http.ResponseWriter, r *http.Request) {
	offset, _ := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
	size, _ := strconv.ParseUint(r.URL.Query().Get("size"), 10, 32)
	expectedChecksum := r.URL.Query().Get("checksum")

	chunkData := make([]byte, size)
	v.volumeMutex.Lock()
	_, err := v.volumeFile.ReadAt(chunkData, int64(offset))
	v.volumeMutex.Unlock()
	if err != nil {
		http.Error(w, "Chunk non lisible", http.StatusInternalServerError)
		return
	}

	hash := sha256.Sum256(chunkData)
	actualChecksum := hex.EncodeToString(hash[:])

	if actualChecksum != expectedChecksum {
		http.Error(w, "Checksum invalide", http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (v *Volume) CleanupOrphanHandler(w http.ResponseWriter, r *http.Request) {
	var cleanupData struct {
		Chunks []struct {
			Checksum string `json:"checksum"`
		} `json:"chunks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&cleanupData); err != nil {
		http.Error(w, "JSON invalide", http.StatusBadRequest)
		return
	}

	cleanedCount := 0
	v.orphanMutex.Lock()
	for _, chunk := range cleanupData.Chunks {
		if _, exists := v.orphanChunks[chunk.Checksum]; exists {
			delete(v.orphanChunks, chunk.Checksum)
			cleanedCount++
		}
	}
	v.orphanMutex.Unlock()

	if cleanedCount > 0 {
		v.saveOrphanChunks()
	}
	w.WriteHeader(http.StatusOK)
}
