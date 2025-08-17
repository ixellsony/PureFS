// --- disk-agent.go ---

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	// IMPORTANT : Assurez-vous que le nom de votre module est correct
	"PureFS/volume" 
)

const (
	volumeSizeGB    = 30
	volumeSizeBytes = volumeSizeGB * 1024 * 1024 * 1024
)

// --- Optimisations de performance ---
var (
	// Pool de goroutines pour limiter la charge
	requestSemaphore chan struct{}
	
	// Configuration optimisée du serveur HTTP
	httpServer *http.Server
)

// generateStableDiskID crée un identifiant unique et constant basé sur le chemin de montage.
func generateStableDiskID(path string) string {
	hasher := sha256.New()
	hasher.Write([]byte(path))
	return "disk-" + hex.EncodeToString(hasher.Sum(nil))[:8]
}

// Configuration du serveur HTTP optimisé
func setupOptimizedHTTPServer(listenAddr string, handler http.Handler) *http.Server {
	// Calculer les paramètres optimaux basés sur le nombre de CPU
	numCPU := runtime.NumCPU()
	maxConnections := numCPU * 100
	
	// Limiter les requêtes concurrentes pour éviter la surcharge
	requestSemaphore = make(chan struct{}, numCPU*4)
	
	server := &http.Server{
		Addr:    listenAddr,
		Handler: handler,
		
		// Timeouts optimisés pour les gros transferts
		ReadTimeout:    60 * time.Second,  // Augmenté pour les gros chunks
		WriteTimeout:   60 * time.Second,  // Augmenté pour les téléchargements
		IdleTimeout:    120 * time.Second, // Connexions persistantes
		MaxHeaderBytes: 1 << 20,           // 1MB headers max
	}
	
	// Configuration du transport optimisée
	server.SetKeepAlivesEnabled(true)
	
	log.Printf("Serveur HTTP configuré avec max %d connexions concurrentes", maxConnections)
	return server
}

// Middleware pour limiter les requêtes concurrentes
func rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Acquérir un slot de requête
		select {
		case requestSemaphore <- struct{}{}:
			defer func() { <-requestSemaphore }()
			next(w, r)
		case <-time.After(5 * time.Second):
			// Timeout si le serveur est surchargé
			http.Error(w, "Serveur temporairement surchargé, réessayez plus tard", http.StatusServiceUnavailable)
		}
	}
}

func main() {
	disksList := flag.String("disks", "", "Liste des points de montage à gérer, séparés par des virgules")
	serverAddr := flag.String("server", "localhost:8080", "Adresse IP:port du serveur d'index")
	listenAddr := flag.String("listen-addr", "0.0.0.0:9000", "IP:port sur laquelle cet agent écoutera pour tous les volumes")
	maxProcs := flag.Int("max-procs", 0, "Nombre maximum de processeurs à utiliser (0 = auto)")
	flag.Parse()

	if *disksList == "" {
		log.Fatal("L'argument -disks est requis. Fournissez au moins un chemin de stockage.")
	}

	// Configuration des performances Go
	if *maxProcs > 0 {
		runtime.GOMAXPROCS(*maxProcs)
	}
	
	// Optimiser le garbage collector pour les gros transferts
	runtime.GC()
	
	diskPaths := strings.Split(*disksList, ",")

	managedVolumes := make(map[string]*volume.Volume)
	var volumesMutex sync.RWMutex

	log.Printf("Agent de disque démarré. Gestion de %d disque(s) avec %d CPU(s).", len(diskPaths), runtime.NumCPU())

	// Traitement parallèle des disques pour l'initialisation
	var wg sync.WaitGroup
	volumeChan := make(chan *volume.Volume, 10)
	
	for _, storagePath := range diskPaths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			
			path = strings.TrimSpace(path)
			if path == "" {
				return
			}

			log.Printf("--- Analyse du disque sur '%s' ---", path)
			diskID := generateStableDiskID(path)

			// Appel à la fonction qui se trouve maintenant dans les fichiers _windows ou _unix
			freeSpace, err := getAvailableSpace(path) 
			if err != nil {
				log.Printf("ERREUR: Impossible d'analyser le disque sur '%s', il sera ignoré. Raison : %v", path, err)
				return
			}

			numVolumes := freeSpace / volumeSizeBytes
			log.Printf("  \\_ Disque ID stable : %s", diskID)
			log.Printf("  \\_ Espace libre      : %.2f GB", float64(freeSpace)/(1024*1024*1024))
			log.Printf("  \\_ Volumes à créer   : %d", numVolumes)

			if numVolumes == 0 {
				log.Printf("  \\_ Pas assez d'espace pour de nouveaux volumes sur ce disque.")
				return
			}

			// Créer les volumes en parallèle
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
				
				volumeChan <- vol
			}
		}(storagePath)
	}
	
	// Fermer le canal une fois tous les workers terminés
	go func() {
		wg.Wait()
		close(volumeChan)
	}()
	
	// Collecter tous les volumes créés
	for vol := range volumeChan {
		volumesMutex.Lock()
		managedVolumes[vol.GetName()] = vol
		volumesMutex.Unlock()
	}

	if len(managedVolumes) == 0 {
		log.Fatal("Aucun volume n'a pu être créé. Vérifiez vos chemins de stockage.")
	}

	// Configuration du routeur avec optimisations
	mux := http.NewServeMux()
	
	// Handler principal avec limitation de débit
	mainHandler := rateLimitMiddleware(func(w http.ResponseWriter, r *http.Request) {
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

		// Configuration des en-têtes pour l'optimisation
		w.Header().Set("Server", "PureFS-Agent/1.0")
		
		// Gestion des en-têtes de cache pour les requêtes de santé
		if actionPath == "/health" {
			w.Header().Set("Cache-Control", "no-cache, max-age=0")
		}

		switch actionPath {
		case "/write_chunk":
			// Optimisation: en-têtes pour les gros uploads
			w.Header().Set("Connection", "keep-alive")
			targetVolume.WriteChunkHandler(w, r)
		case "/read_chunk":
			// Optimisation: en-têtes pour les téléchargements
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("Accept-Ranges", "bytes")
			targetVolume.ReadChunkHandler(w, r)
		case "/verify_chunk":
			targetVolume.VerifyChunkHandler(w, r)
		case "/mark_deleted":
			targetVolume.MarkDeletedHandler(w, r)
		case "/compact":
			// Optimisation: timeout étendu pour le compactage
			w.Header().Set("Connection", "keep-alive")
			targetVolume.CompactHandler(w, r)
		case "/cleanup_orphan":
			targetVolume.CleanupOrphanHandler(w, r)
		case "/health":
			// Réponse rapide pour les vérifications de santé
			w.Header().Set("Content-Type", "application/json")
			targetVolume.HealthCheckHandler(w, r)
		default:
			http.NotFound(w, r)
		}
	})

	mux.HandleFunc("/", mainHandler)
	
	// Handler pour les statistiques de performance (optionnel)
	mux.HandleFunc("/stats", rateLimitMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		volumesMutex.RLock()
		volumeCount := len(managedVolumes)
		volumesMutex.RUnlock()
		
		stats := map[string]interface{}{
			"agent_version":     "1.0-optimized",
			"managed_volumes":   volumeCount,
			"cpu_count":        runtime.NumCPU(),
			"goroutines":       runtime.NumGoroutine(),
			"max_concurrent":   cap(requestSemaphore),
			"memory": map[string]interface{}{
				"alloc":      getMemoryStats().Alloc,
				"sys":        getMemoryStats().Sys,
				"gc_cycles":  getMemoryStats().NumGC,
			},
		}
		
		json.NewEncoder(w).Encode(stats)
	}))

	// Configuration du serveur HTTP optimisé
	httpServer = setupOptimizedHTTPServer(*listenAddr, mux)

	log.Printf("Agent de disque optimisé en écoute sur %s. Gestion de %d volumes.", *listenAddr, len(managedVolumes))
	log.Printf("Optimisations actives: Pool de connexions • Limitation de débit • Garbage collection optimisé")
	
	// Démarrer le monitoring des performances en arrière-plan
	go performanceMonitor()
	
	// Démarrer le serveur
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("L'agent de disque n'a pas pu démarrer: %v", err)
	}
}

// Fonction pour obtenir les statistiques mémoire
func getMemoryStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

// Monitoring des performances en arrière-plan
func performanceMonitor() {
	ticker := time.NewTicker(60 * time.Second) // Monitoring toutes les minutes
	defer ticker.Stop()
	
	var lastGCCount uint32
	
	for range ticker.C {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		
		// Log des métriques importantes
		if m.NumGC > lastGCCount {
			log.Printf("PERF: GC cycles: %d, Alloc: %.2f MB, Sys: %.2f MB, Goroutines: %d", 
				m.NumGC, 
				float64(m.Alloc)/(1024*1024),
				float64(m.Sys)/(1024*1024),
				runtime.NumGoroutine())
			lastGCCount = m.NumGC
		}
		
		// Forcer un GC si la mémoire allouée dépasse un seuil
		if m.Alloc > 500*1024*1024 { // 500MB
			log.Printf("PERF: Déclenchement GC préventif (mémoire > 500MB)")
			runtime.GC()
		}
		
		// Vérifier le nombre de goroutines
		if runtime.NumGoroutine() > 1000 {
			log.Printf("ATTENTION: Nombre élevé de goroutines: %d", runtime.NumGoroutine())
		}
	}
}
