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

var (
	diskConfig     Config
	volumePath     string
	volumeMutex    = &sync.Mutex{}
	deletedChunks  = make(map[uint64]uint32) // offset -> size
	deletedMutex   = &sync.RWMutex{}
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

	if err := initialRegister(); err != nil {
		log.Fatalf("Impossible de démarrer le volume. Erreur d'enregistrement : %v", err)
	}

	go registerWithServer()

	http.HandleFunc("/write_chunk", writeChunkHandler)
	http.HandleFunc("/read_chunk", readChunkHandler)
	http.HandleFunc("/mark_deleted", markDeletedHandler)
	http.HandleFunc("/compact", compactHandler)

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

func saveDeletedChunks() {
	deletedPath := filepath.Join(diskConfig.Storage, fmt.Sprintf("%s.deleted", diskConfig.Name))
	
	deletedMutex.RLock()
	var chunks []DeletedChunk
	for offset, size := range deletedChunks {
		chunks = append(chunks, DeletedChunk{Offset: offset, Size: size})
	}
	deletedMutex.RUnlock()

	file, err := os.Create(deletedPath)
	if err != nil {
		log.Printf("Erreur lors de la sauvegarde des chunks supprimés : %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(chunks); err != nil {
		log.Printf("Erreur lors de l'encodage des chunks supprimés : %v", err)
	}
}

func getFreeSpaceBytes() uint64 {
	totalBytes := uint64(volumeSizeGB) * 1024 * 1024 * 1024

	fileInfo, err := os.Stat(volumePath)
	if err != nil {
		return totalBytes
	}

	usedBytes := uint64(fileInfo.Size())
	
	// Soustraire l'espace des chunks supprimés (qui sera libéré au prochain compactage)
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

func compactVolume() error {
	log.Printf("Début du compactage du volume '%s'", diskConfig.Name)
	
	deletedMutex.RLock()
	if len(deletedChunks) == 0 {
		deletedMutex.RUnlock()
		log.Printf("Aucun chunk à supprimer, compactage ignoré")
		return nil
	}
	
	// Créer une liste triée des zones supprimées
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
	
	// Créer un fichier temporaire pour le volume compacté
	tempPath := volumePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("impossible de créer le fichier temporaire : %v", err)
	}
	defer tempFile.Close()
	
	originalFile, err := os.Open(volumePath)
	if err != nil {
		return fmt.Errorf("impossible d'ouvrir le fichier original : %v", err)
	}
	defer originalFile.Close()
	
	var currentPos uint64 = 0
	deletedIdx := 0
	
	for {
		// Trouver la prochaine région à ignorer
		var nextDeletedStart uint64 = ^uint64(0) // Max uint64
		if deletedIdx < len(deletedRegions) {
			nextDeletedStart = deletedRegions[deletedIdx].offset
		}
		
		// Copier les données jusqu'à la prochaine région supprimée
		if currentPos < nextDeletedStart {
			copySize := nextDeletedStart - currentPos
			
			// Lire par blocs pour éviter de charger tout en mémoire
			buffer := make([]byte, 1024*1024) // 1MB buffer
			var totalCopied uint64 = 0
			
			for totalCopied < copySize {
				readSize := uint64(len(buffer))
				if totalCopied + readSize > copySize {
					readSize = copySize - totalCopied
				}
				
				n, err := originalFile.ReadAt(buffer[:readSize], int64(currentPos + totalCopied))
				if err != nil && err != io.EOF {
					return fmt.Errorf("erreur de lecture : %v", err)
				}
				if n == 0 {
					break
				}
				
				if _, err := tempFile.Write(buffer[:n]); err != nil {
					return fmt.Errorf("erreur d'écriture : %v", err)
				}
				
				totalCopied += uint64(n)
			}
			
			currentPos = nextDeletedStart
		}
		
		// Ignorer la région supprimée
		if deletedIdx < len(deletedRegions) {
			currentPos += uint64(deletedRegions[deletedIdx].size)
			deletedIdx++
		} else {
			break
		}
	}
	
	// Fermer les fichiers avant le remplacement
	originalFile.Close()
	tempFile.Close()
	
	// Remplacer l'ancien fichier par le nouveau
	if err := os.Rename(tempPath, volumePath); err != nil {
		os.Remove(tempPath) // Nettoyer en cas d'erreur
		return fmt.Errorf("impossible de remplacer le fichier original : %v", err)
	}
	
	// Vider la liste des chunks supprimés
	deletedMutex.Lock()
	deletedCount := len(deletedChunks)
	deletedChunks = make(map[uint64]uint32)
	deletedMutex.Unlock()
	
	saveDeletedChunks()
	
	log.Printf("Compactage terminé : %d chunks supprimés, fichier optimisé", deletedCount)
	return nil
}

// --- Handlers HTTP ---

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
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}

	bytesWritten, err := file.Write(chunkData)
	if err != nil {
		http.Error(w, "Erreur lors de l'écriture du chunk", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{"offset": offset, "size": uint32(bytesWritten)}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
	log.Printf("Chunk écrit (checksum OK: %s...): taille %d, offset %d", actualChecksum[:8], bytesWritten, offset)
}

func readChunkHandler(w http.ResponseWriter, r *http.Request) {
	var offset, size int64
	_, errO := fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)
	_, errS := fmt.Sscanf(r.URL.Query().Get("size"), "%d", &size)
	if errO != nil || errS != nil || size <= 0 {
		http.Error(w, "Paramètres 'offset' et 'size' invalides", http.StatusBadRequest)
		return
	}

	// Vérifier si ce chunk est marqué comme supprimé
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
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	chunkData := make([]byte, size)
	_, err = file.ReadAt(chunkData, offset)
	if err != nil {
		http.Error(w, "Offset de lecture invalide ou erreur de lecture", http.StatusInternalServerError)
		return
	}

	hash := sha256.Sum256(chunkData)
	actualChecksum := hex.EncodeToString(hash[:])
	w.Header().Set("X-Chunk-Checksum", actualChecksum)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	w.Write(chunkData)
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

func compactHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
		return
	}

	// Répondre immédiatement
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "Compactage démarré en arrière-plan",
	})

	// Lancer le compactage en arrière-plan
	go func() {
		if err := compactVolume(); err != nil {
			log.Printf("Erreur lors du compactage : %v", err)
		}
	}()
}
