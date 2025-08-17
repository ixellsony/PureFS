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

// --- Pools d'optimisation ---
var (
	// Pool de buffers pour éviter les allocations répétées
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 64*1024) // Buffer de 64KB
		},
	}
	
	// Pool de clients HTTP
	httpClientPool = sync.Pool{
		New: func() interface{} {
			return &http.Client{
				Timeout: 30 * time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 5,
					IdleConnTimeout:     60 * time.Second,
				},
			}
		},
	}
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
	
	// Pool de descripteurs de fichiers pour optimiser les I/O
	fileHandles   []*os.File
	filePoolMutex sync.RWMutex
	currentHandle int
	
	// Optimisation: pas de mutex global, utilisation de RWMutex pour les métadonnées
	deletedChunks map[uint64]uint32
	deletedMutex  sync.RWMutex
	orphanChunks  map[string]OrphanChunk
	orphanMutex   sync.RWMutex
	
	// Channel pour la synchronisation asynchrone
	syncRequests chan struct{}
}

// New crée et initialise une nouvelle instance de Volume.
func New(cfg Config) (*Volume, error) {
	v := &Volume{
		config:        cfg,
		deletedChunks: make(map[uint64]uint32),
		orphanChunks:  make(map[string]OrphanChunk),
		syncRequests:  make(chan struct{}, 10), // Buffer pour les demandes de sync
	}

	volumeFileName := fmt.Sprintf("%s.dat", v.config.Name)
	v.volumePath = filepath.Join(v.config.StorageDir, volumeFileName)

	log.Printf("Initialisation du volume '%s' sur le disque '%s'", v.config.Name, v.config.DiskID)

	if err := v.ensureVolumeFile(); err != nil {
		return nil, fmt.Errorf("erreur lors de la création du fichier pour le volume %s: %w", v.config.Name, err)
	}

	// Créer un pool de descripteurs de fichiers pour optimiser les I/O
	if err := v.initFileHandlePool(); err != nil {
		return nil, fmt.Errorf("impossible d'initialiser le pool de descripteurs de fichiers: %w", err)
	}

	v.loadDeletedChunks()
	v.loadOrphanChunks()

	return v, nil
}

// Initialiser le pool de descripteurs de fichiers
func (v *Volume) initFileHandlePool() error {
	const poolSize = 4 // 4 descripteurs pour les opérations parallèles
	v.fileHandles = make([]*os.File, poolSize)
	
	for i := 0; i < poolSize; i++ {
		file, err := os.OpenFile(v.volumePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			// Nettoyer les descripteurs déjà ouverts en cas d'erreur
			for j := 0; j < i; j++ {
				if v.fileHandles[j] != nil {
					v.fileHandles[j].Close()
				}
			}
			return fmt.Errorf("impossible d'ouvrir le descripteur de fichier %d: %w", i, err)
		}
		v.fileHandles[i] = file
	}
	
	return nil
}

// Obtenir un descripteur de fichier du pool
func (v *Volume) getFileHandle() *os.File {
	v.filePoolMutex.Lock()
	defer v.filePoolMutex.Unlock()
	
	handle := v.fileHandles[v.currentHandle]
	v.currentHandle = (v.currentHandle + 1) % len(v.fileHandles)
	return handle
}

// Start lance les tâches de fond du volume (heartbeats). C'est une méthode non-bloquante.
func (v *Volume) Start() error {
	log.Printf("Démarrage de la logique du volume '%s'...", v.config.Name)

	if err := v.initialRegister(); err != nil {
		v.cleanup() // Nettoyage en cas d'échec
		return fmt.Errorf("impossible de démarrer le volume '%s'. Erreur d'enregistrement : %w", v.config.Name, err)
	}

	// Démarrer les workers de fond
	go v.cleanupOldOrphans()
	go v.registerWithServer() // Tâche de fond pour les heartbeats
	go v.syncWorker()         // Worker pour les synchronisations asynchrones

	log.Printf("Volume '%s' est prêt et envoie des heartbeats.", v.config.Name)
	return nil
}

// Worker pour gérer les synchronisations de manière asynchrone
func (v *Volume) syncWorker() {
	ticker := time.NewTicker(5 * time.Second) // Sync toutes les 5 secondes
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			// Sync périodique
			v.performSync()
		case <-v.syncRequests:
			// Sync sur demande (non-bloquant)
			v.performSync()
		}
	}
}

// Effectuer la synchronisation sur tous les descripteurs de fichiers
func (v *Volume) performSync() {
	v.filePoolMutex.RLock()
	defer v.filePoolMutex.RUnlock()
	
	for _, handle := range v.fileHandles {
		if handle != nil {
			if err := handle.Sync(); err != nil {
				log.Printf("ERREUR SYNC (vol %s): %v", v.config.Name, err)
			}
		}
	}
}

// Demander une synchronisation de manière non-bloquante
func (v *Volume) requestSync() {
	select {
	case v.syncRequests <- struct{}{}:
		// Demande de sync envoyée
	default:
		// Channel plein, ignore (le sync périodique s'en chargera)
	}
}

// Nettoyage des ressources
func (v *Volume) cleanup() {
	v.filePoolMutex.Lock()
	defer v.filePoolMutex.Unlock()
	
	for _, handle := range v.fileHandles {
		if handle != nil {
			handle.Close()
		}
	}
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

	// Utiliser un handle du pool pour vérifier la taille
	handle := v.getFileHandle()
	fileInfo, err := handle.Stat()
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
		"address":    v.config.ListenAddr,
		"totalSpace": uint64(volumeSizeGB) * 1024 * 1024 * 1024,
		"freeSpace":  v.getFreeSpaceBytes(),
	}
	return json.Marshal(status)
}

func (v *Volume) getHTTPClient() *http.Client {
	return httpClientPool.Get().(*http.Client)
}

func (v *Volume) putHTTPClient(client *http.Client) {
	httpClientPool.Put(client)
}

func (v *Volume) initialRegister() error {
	payload, err := v.buildStatusPayload("initial")
	if err != nil {
		return fmt.Errorf("impossible de construire la charge utile JSON : %w", err)
	}

	serverURL := fmt.Sprintf("http://%s/api/disk/register", v.config.ServerAddr)
	
	client := v.getHTTPClient()
	defer v.putHTTPClient(client)
	
	resp, err := client.Post(serverURL, "application/json", bytes.NewBuffer(payload))
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
		
		client := v.getHTTPClient()
		resp, err := client.Post(serverURL, "application/json", bytes.NewBuffer(payload))
		v.putHTTPClient(client)
		
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

// --- Handlers Optimisés (maintenant des méthodes publiques appelées par l'agent) ---

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

	// Utiliser un buffer du pool pour la lecture
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	// Lire les données avec un buffer optimisé
	var chunkData []byte
	var totalRead int
	
	for {
		n, err := r.Body.Read(buffer)
		if n > 0 {
			chunkData = append(chunkData, buffer[:n]...)
			totalRead += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "Erreur lors de la lecture du chunk", http.StatusInternalServerError)
			return
		}
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

	// Obtenir un descripteur de fichier du pool
	handle := v.getFileHandle()
	
	offset, err := handle.Seek(0, io.SeekEnd)
	if err != nil {
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}

	bytesWritten, err := handle.Write(chunkData)
	if err != nil {
		// En cas d'erreur, marquer comme orphelin
		v.orphanMutex.Lock()
		v.orphanChunks[actualChecksum] = OrphanChunk{
			Checksum: actualChecksum, 
			Offset:   uint64(offset), 
			Size:     uint32(len(chunkData)), 
			Created:  time.Now(),
		}
		v.orphanMutex.Unlock()
		v.saveOrphanChunks()
		http.Error(w, "Erreur lors de l'écriture du chunk", http.StatusInternalServerError)
		return
	}

	// Demander une synchronisation asynchrone au lieu d'un sync bloquant
	v.requestSync()

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

	// Vérification rapide de suppression avec RLock
	v.deletedMutex.RLock()
	if _, isDeleted := v.deletedChunks[uint64(offset)]; isDeleted {
		v.deletedMutex.RUnlock()
		http.Error(w, "Chunk supprimé", http.StatusNotFound)
		return
	}
	v.deletedMutex.RUnlock()

	// Obtenir un descripteur de fichier du pool
	handle := v.getFileHandle()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))

	// Utiliser un buffer du pool pour la lecture
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	// Streaming direct pour éviter de charger tout en mémoire
	reader := io.NewSectionReader(handle, offset, size)
	_, err := io.CopyBuffer(w, reader, buffer)
	
	if err != nil {
		log.Printf("Erreur de lecture/streaming du chunk (vol %s): %v", v.config.Name, err)
	}
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

	// Fermer tous les descripteurs pendant le compactage
	v.filePoolMutex.Lock()
	for _, handle := range v.fileHandles {
		if handle != nil {
			handle.Close()
		}
	}
	v.filePoolMutex.Unlock()

	// Ouvrir le fichier original en lecture
	originalFile, err := os.Open(v.volumePath)
	if err != nil {
		v.initFileHandlePool() // Réinitialiser le pool en cas d'erreur
		return nil, fmt.Errorf("impossible d'ouvrir le fichier original: %w", err)
	}
	defer originalFile.Close()

	// Créer le fichier temporaire
	tempPath := v.volumePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		v.initFileHandlePool()
		return nil, fmt.Errorf("impossible de créer le fichier temporaire: %w", err)
	}
	defer tempFile.Close()

	offsetMapping := make(map[uint64]uint64)
	var currentWriteOffset uint64 = 0

	// Utiliser un buffer optimisé pour la copie
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)

	for _, chunk := range chunksToKeep {
		offsetMapping[chunk.Offset] = currentWriteOffset
		
		// Copie optimisée avec buffer
		reader := io.NewSectionReader(originalFile, int64(chunk.Offset), int64(chunk.Size))
		bytesCopied, err := io.CopyBuffer(tempFile, reader, buffer)
		
		if err != nil {
			os.Remove(tempPath)
			v.initFileHandlePool()
			return nil, fmt.Errorf("erreur de copie du chunk: %w", err)
		}
		currentWriteOffset += uint64(bytesCopied)
	}

	// Fermer les fichiers avant le remplacement
	originalFile.Close()
	tempFile.Close()

	// Remplacer le fichier original
	if err := os.Rename(tempPath, v.volumePath); err != nil {
		os.Remove(tempPath)
		v.initFileHandlePool()
		return nil, fmt.Errorf("impossible de remplacer le fichier original: %w", err)
	}

	// Réinitialiser le pool de descripteurs
	if err := v.initFileHandlePool(); err != nil {
		log.Fatalf("ERREUR CRITIQUE (vol %s): Impossible de réinitialiser le pool de descripteurs après compactage: %v", v.config.Name, err)
	}

	// Nettoyer les chunks supprimés
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
	
	// Sauvegarde asynchrone pour ne pas bloquer la réponse
	go v.saveDeletedChunks()
	
	w.WriteHeader(http.StatusOK)
}

// GetName retourne le nom du volume (nécessaire pour l'agent)
func (v *Volume) GetName() string {
	return v.config.Name
}

func (v *Volume) VerifyChunkHandler(w http.ResponseWriter, r *http.Request) {
	offset, _ := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
	size, _ := strconv.ParseUint(r.URL.Query().Get("size"), 10, 32)
	expectedChecksum := r.URL.Query().Get("checksum")

	// Vérification rapide de suppression
	v.deletedMutex.RLock()
	if _, isDeleted := v.deletedChunks[offset]; isDeleted {
		v.deletedMutex.RUnlock()
		http.Error(w, "Chunk supprimé", http.StatusNotFound)
		return
	}
	v.deletedMutex.RUnlock()

	// Utiliser un buffer du pool
	buffer := bufferPool.Get().([]byte)
	defer bufferPool.Put(buffer)
	
	// Redimensionner le buffer si nécessaire
	chunkData := make([]byte, size)
	
	handle := v.getFileHandle()
	_, err := handle.ReadAt(chunkData, int64(offset))
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
		// Sauvegarde asynchrone
		go v.saveOrphanChunks()
	}
	
	w.WriteHeader(http.StatusOK)
}
