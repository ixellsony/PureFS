// --- disk-agent.go ---

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	// IMPORTANT : Assurez-vous que le nom de votre module est correct
	"PureFS/volume" 
)

const (
	volumeSizeGB    = 30
	volumeSizeBytes = volumeSizeGB * 1024 * 1024 * 1024
)

// La fonction getAvailableSpace n'est PLUS ici. Elle est dans ses propres fichiers.

// generateStableDiskID crée un identifiant unique et constant basé sur le chemin de montage.
func generateStableDiskID(path string) string {
	hasher := sha256.New()
	hasher.Write([]byte(path))
	return "disk-" + hex.EncodeToString(hasher.Sum(nil))[:8]
}

func main() {
	disksList := flag.String("disks", "", "Liste des points de montage à gérer, séparés par des virgules")
	serverAddr := flag.String("server", "localhost:8080", "Adresse IP:port du serveur d'index")
	listenAddr := flag.String("listen-addr", "0.0.0.0:9000", "IP:port sur laquelle cet agent écoutera pour tous les volumes")
	flag.Parse()

	if *disksList == "" {
		log.Fatal("L'argument -disks est requis. Fournissez au moins un chemin de stockage.")
	}

	diskPaths := strings.Split(*disksList, ",")

	managedVolumes := make(map[string]*volume.Volume)
	var volumesMutex sync.RWMutex

	log.Printf("Agent de disque démarré. Gestion de %d disque(s).", len(diskPaths))

	for _, storagePath := range diskPaths {
		path := strings.TrimSpace(storagePath)
		if path == "" {
			continue
		}

		log.Printf("--- Analyse du disque sur '%s' ---", path)
		diskID := generateStableDiskID(path)

		// Appel à la fonction qui se trouve maintenant dans les fichiers _windows ou _unix
		freeSpace, err := getAvailableSpace(path) 
		if err != nil {
			log.Printf("ERREUR: Impossible d'analyser le disque sur '%s', il sera ignoré. Raison : %v", path, err)
			continue
		}

		numVolumes := freeSpace / volumeSizeBytes
		log.Printf("  \\_ Disque ID stable : %s", diskID)
		log.Printf("  \\_ Espace libre      : %.2f GB", float64(freeSpace)/(1024*1024*1024))
		log.Printf("  \\_ Volumes à créer   : %d", numVolumes)

		if numVolumes == 0 {
			log.Printf("  \\_ Pas assez d'espace pour de nouveaux volumes sur ce disque.")
			continue
		}

		for i := 0; i < int(numVolumes); i++ {
			volumeName := fmt.Sprintf("%s-vol-%03d", diskID, i+1)
			cfg := volume.Config{
				Name:       volumeName,
				DiskID:     diskID,
				StorageDir: path,
				ServerAddr: *serverAddr,
				ListenAddr: *listenAddr,
			}

			vol, err := volume.New(cfg)
			if err != nil {
				log.Printf("ERREUR: Impossible d'initialiser le volume %s: %v", volumeName, err)
				continue
			}

			if err := vol.Start(); err != nil {
				log.Printf("ERREUR: Le volume %s n'a pas pu démarrer sa logique interne : %v", volumeName, err)
				continue
			}
			
			volumesMutex.Lock()
			managedVolumes[volumeName] = vol
			volumesMutex.Unlock()
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(strings.Trim(r.URL.Path, "/"), "/", 2)
		if len(parts) < 2 {
			http.Error(w, "URL Invalide. Format attendu: /<volume_name>/<action>", http.StatusBadRequest)
			return
		}

		volumeName := parts[0]
		actionPath := "/" + parts[1]

		volumesMutex.RLock()
		targetVolume, exists := managedVolumes[volumeName]
		volumesMutex.RUnlock()

		if !exists {
			log.Printf("Avertissement: Requête reçue pour un volume inconnu ou non géré par cet agent: %s", volumeName)
			http.NotFound(w, r)
			return
		}

		switch actionPath {
		case "/write_chunk":
			targetVolume.WriteChunkHandler(w, r)
		case "/read_chunk":
			targetVolume.ReadChunkHandler(w, r)
		case "/verify_chunk":
			targetVolume.VerifyChunkHandler(w, r)
		case "/mark_deleted":
			targetVolume.MarkDeletedHandler(w, r)
		case "/compact":
			targetVolume.CompactHandler(w, r)
		case "/cleanup_orphan":
			targetVolume.CleanupOrphanHandler(w, r)
		case "/health":
			targetVolume.HealthCheckHandler(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	log.Printf("Agent de disque en écoute sur %s. Gestion de %d volumes.", *listenAddr, len(managedVolumes))
	if err := http.ListenAndServe(*listenAddr, mux); err != nil {
		log.Fatalf("L'agent de disque n'a pas pu démarrer: %v", err)
	}
}
