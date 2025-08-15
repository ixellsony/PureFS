// volume.go
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// --- Constantes et Configuration ---
const (
	volumeSizeGB = 30
)

type Config struct {
	Name    string
	Storage string
	Server  string
	Address string
}

var (
	diskConfig  Config
	volumePath  string
	volumeMutex = &sync.Mutex{}
)

// --- Fonctions Principales ---

func main() {
	name := flag.String("name", "", "Nom unique du disque (requis)")
	storage := flag.String("storage", ".", "Emplacement de stockage pour le fichier de volume")
	server := flag.String("server", "localhost:8080", "Adresse IP:port du serveur d'index")
	address := flag.String("address", "localhost:9000", "Adresse IP:port de ce disque pour écouter")
	flag.Parse()

	if *name == "" {
		log.Fatal("L'argument -name est requis.")
	}

	diskConfig = Config{
		Name:    *name,
		Storage: *storage,
		Server:  *server,
		Address: *address,
	}

	volumeFileName := fmt.Sprintf("%s.dat", diskConfig.Name)
	volumePath = filepath.Join(diskConfig.Storage, volumeFileName)

	log.Printf("Démarrage du disque '%s' sur %s", diskConfig.Name, diskConfig.Address)
	log.Printf("Utilisation du fichier de volume : %s", volumePath)

	ensureVolumeFile()

	if err := initialRegister(); err != nil {
		log.Fatalf("Impossible de démarrer le volume. Erreur d'enregistrement : %v", err)
	}

	go registerWithServer()

	http.HandleFunc("/write_chunk", writeChunkHandler)
	http.HandleFunc("/read_chunk", readChunkHandler)

	log.Printf("Disque '%s' en écoute sur http://%s", diskConfig.Name, diskConfig.Address)
	if err := http.ListenAndServe(diskConfig.Address, nil); err != nil {
		log.Fatalf("Le serveur du disque n'a pas pu démarrer : %v", err)
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

func getFreeSpaceBytes() uint64 {
	totalBytes := uint64(volumeSizeGB) * 1024 * 1024 * 1024

	fileInfo, err := os.Stat(volumePath)
	if err != nil {
		return totalBytes
	}

	usedBytes := uint64(fileInfo.Size())
	if usedBytes >= totalBytes {
		return 0
	}
	return totalBytes - usedBytes
}

func initialRegister() error {
	status := map[string]interface{}{
		"name":       diskConfig.Name,
		"address":    diskConfig.Address,
		"totalSpace": uint64(volumeSizeGB) * 1024 * 1024 * 1024,
		"freeSpace":  getFreeSpaceBytes(),
	}

	payload, err := json.Marshal(status)
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
		status := map[string]interface{}{
			"name":       diskConfig.Name,
			"address":    diskConfig.Address,
			"totalSpace": uint64(volumeSizeGB) * 1024 * 1024 * 1024,
			"freeSpace":  getFreeSpaceBytes(),
		}

		payload, _ := json.Marshal(status)
		serverURL := fmt.Sprintf("http://%s/api/disk/register", diskConfig.Server)

		resp, err := http.Post(serverURL, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("Erreur de connexion au serveur %s : %v", diskConfig.Server, err)
		} else {
			if resp.StatusCode != http.StatusOK {
				bodyBytes, _ := io.ReadAll(resp.Body)
				log.Printf("Le serveur a répondu avec un statut non-OK lors du heartbeat : %s. Réponse : %s", resp.Status, string(bodyBytes))
			} else {
				log.Printf("Heartbeat envoyé au serveur avec succès.")
			}
			resp.Body.Close()
		}
	}
}

// --- Handlers HTTP ---

func writeChunkHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Méthode non autorisée", http.StatusMethodNotAllowed)
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

	bytesWritten, err := io.Copy(file, r.Body)
	if err != nil {
		http.Error(w, "Erreur lors de l'écriture du chunk", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"offset": offset,
		"size":   bytesWritten,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Printf("Chunk écrit avec succès (taille: %d, offset: %d)", bytesWritten, offset)
}

func readChunkHandler(w http.ResponseWriter, r *http.Request) {
	var offset, size int64
	// CORRECTION: Utilisation de '=' au lieu de ':=' car les variables existent déjà.
	_, errO := fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)
	_, errS := fmt.Sscanf(r.URL.Query().Get("size"), "%d", &size)

	if errO != nil || errS != nil || size <= 0 {
		http.Error(w, "Paramètres 'offset' et 'size' invalides", http.StatusBadRequest)
		return
	}

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	file, err := os.Open(volumePath)
	if err != nil {
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		http.Error(w, "Offset de lecture invalide", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
	io.CopyN(w, file, size)
}
