// disk.go
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
	volumeFileName = "volume.dat"
	// On définit la taille du volume pour les rapports, mais on ne pré-alloue pas pour la simplicité.
	volumeSizeGB   = 30
)

// Config contient les paramètres du disque.
type Config struct {
	Name    string
	Storage string
	Server  string
	Address string
}

// Global state for the disk node
var (
	diskConfig  Config
	volumePath  string
	volumeMutex = &sync.Mutex{} // Protège l'accès au fichier volume.dat
)

// --- Fonctions Principales ---

func main() {
	// 1. Parsing des arguments
	name := flag.String("name", "", "Nom unique du disque (requis)")
	storage := flag.String("storage", ".", "Emplacement de stockage pour volume.dat")
	server := flag.String("server", "localhost:8080", "Adresse IP:port du serveur d'index")
	address := flag.String("address", "localhost:9000", "Adresse IP:port de ce disque pour écouter")
	flag.Parse()

	if *name == "" {
		log.Fatal("L'argument -name est requis.")
	}

	diskConfig = Config{
		Name: *name, Storage: *storage, Server: *server, Address: *address,
	}
	volumePath = filepath.Join(diskConfig.Storage, volumeFileName)

	log.Printf("Démarrage du disque '%s' sur %s", diskConfig.Name, diskConfig.Address)

	// 2. Préparer le fichier de volume
	ensureVolumeFile()

	// 3. Lancer le "heartbeat" pour s'enregistrer auprès du serveur
	go registerWithServer()

	// 4. Démarrer le serveur HTTP de ce disque pour écouter les ordres
	http.HandleFunc("/write_chunk", writeChunkHandler)
	http.HandleFunc("/read_chunk", readChunkHandler)

	log.Printf("Disque '%s' en écoute sur http://%s", diskConfig.Name, diskConfig.Address)
	if err := http.ListenAndServe(diskConfig.Address, nil); err != nil {
		log.Fatalf("Le serveur du disque n'a pas pu démarrer: %v", err)
	}
}

// --- Logique Métier ---

func ensureVolumeFile() {
	if err := os.MkdirAll(diskConfig.Storage, 0755); err != nil {
		log.Fatalf("Impossible de créer le répertoire de stockage: %v", err)
	}
	
	// Si le fichier n'existe pas, on le crée.
	if _, err := os.Stat(volumePath); os.IsNotExist(err) {
		log.Printf("Création du fichier de volume: %s", volumePath)
		file, err := os.Create(volumePath)
		if err != nil {
			log.Fatalf("Impossible de créer le fichier de volume: %v", err)
		}
		file.Close()
	} else {
		log.Printf("Fichier de volume existant trouvé: %s", volumePath)
	}
}

func getFreeSpaceGB() float64 {
	fileInfo, err := os.Stat(volumePath)
	if err != nil {
		return float64(volumeSizeGB) // Retourne la taille totale si erreur
	}
	usedBytes := fileInfo.Size()
	totalBytes := int64(volumeSizeGB) * 1024 * 1024 * 1024
	freeBytes := totalBytes - usedBytes
	if freeBytes < 0 {
		freeBytes = 0
	}
	return float64(freeBytes) / (1024 * 1024 * 1024)
}

func registerWithServer() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Enregistrement immédiat au démarrage, puis toutes les 30s
	for ; ; {
		status := map[string]interface{}{
			"name":      diskConfig.Name,
			"address":   diskConfig.Address,
			"totalSpace": volumeSizeGB,
			"freeSpace": getFreeSpaceGB(),
		}

		payload, _ := json.Marshal(status)
		serverURL := fmt.Sprintf("http://%s/api/disk/register", diskConfig.Server)
		
		resp, err := http.Post(serverURL, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("Erreur de connexion au serveur %s: %v", diskConfig.Server, err)
		} else {
			if resp.StatusCode != http.StatusOK {
				log.Printf("Le serveur a répondu avec un statut non-OK: %s", resp.Status)
			} else {
				log.Printf("Heartbeat envoyé au serveur avec succès.")
			}
			resp.Body.Close()
		}
		
		<-ticker.C
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

	// Ouvrir en mode ajout (append)
	file, err := os.OpenFile(volumePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("ERREUR: impossible d'ouvrir volume.dat: %v", err)
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// L'offset est la taille actuelle du fichier AVANT l'écriture
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		log.Printf("ERREUR: impossible de trouver la fin de volume.dat: %v", err)
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}

	// Copier les données de la requête directement dans le fichier
	bytesWritten, err := io.Copy(file, r.Body)
	if err != nil {
		log.Printf("ERREUR: écriture du chunk échouée: %v", err)
		http.Error(w, "Erreur lors de l'écriture du chunk", http.StatusInternalServerError)
		return
	}
	
	// Renvoyer l'offset et la taille écrite au serveur
	response := map[string]interface{}{
		"offset": offset,
		"size":   bytesWritten,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
	
	log.Printf("Chunk écrit avec succès (taille: %d, offset: %d)", bytesWritten, offset)
}

func readChunkHandler(w http.ResponseWriter, r *http.Request) {
	// Extraire offset et size des paramètres de l'URL
	offset, err1 := r.URL.Query().Get("offset"), error(nil)
	size, err2 := r.URL.Query().Get("size"), error(nil)

	var o, s int64
	fmt.Sscanf(offset, "%d", &o)
	fmt.Sscanf(size, "%d", &s)
	
	if err1 != nil || err2 != nil || s == 0 {
		http.Error(w, "Paramètres 'offset' et 'size' invalides", http.StatusBadRequest)
		return
	}

	volumeMutex.Lock()
	defer volumeMutex.Unlock()

	file, err := os.Open(volumePath)
	if err != nil {
		log.Printf("ERREUR: impossible d'ouvrir volume.dat pour lecture: %v", err)
		http.Error(w, "Erreur interne du disque", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Se positionner à l'offset demandé
	_, err = file.Seek(o, io.SeekStart)
	if err != nil {
		log.Printf("ERREUR: impossible de se positionner à l'offset %d: %v", o, err)
		http.Error(w, "Offset de lecture invalide", http.StatusInternalServerError)
		return
	}

	// Streamer exactement 'size' bytes depuis le fichier vers la réponse
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", s))
	n, err := io.CopyN(w, file, s)
	if err != nil && err != io.EOF {
		log.Printf("ERREUR: lecture du chunk (offset: %d, size: %d) échouée après %d bytes: %v", o, s, n, err)
	}
}
