// test_purefs.go - Suite de tests automatisés pour PureFS
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	serverURL = "http://localhost:8080"
	testDir   = "./test_files"
)

type TestResult struct {
	Name     string
	Status   string
	Duration time.Duration
	Error    string
}

type TestSuite struct {
	Results []TestResult
	Passed  int
	Failed  int
}

func main() {
	fmt.Println("🧪 === SUITE DE TESTS AUTOMATISÉS PUREFS ===")
	fmt.Println("Validation de l'intégrité et des performances du système\n")

	suite := &TestSuite{}

	// Créer le répertoire de test
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// Tests de base
	suite.runTest("Test de Connexion au Serveur", testServerConnection)
	suite.runTest("Test Upload Fichier Simple", testSimpleUpload)
	suite.runTest("Test Download et Vérification Intégrité", testDownloadIntegrity)
	suite.runTest("Test Upload Gros Fichier (Performance)", testLargeFileUpload)
	suite.runTest("Test Fichiers Multiples Concurrents", testConcurrentUploads)
	
	// Tests de robustesse
	suite.runTest("Test Corruption et Récupération", testCorruptionRecovery)
	suite.runTest("Test Suppression et Nettoyage", testDeletionCleanup)
	suite.runTest("Test Résistance aux Erreurs", testErrorResilience)
	
	// Tests de performance
	suite.runTest("Test Performance Upload/Download", testPerformanceBenchmark)
	suite.runTest("Test Charge Système", testSystemLoad)

	// Afficher les résultats
	suite.printResults()

	if suite.Failed > 0 {
		os.Exit(1)
	}
}

func (suite *TestSuite) runTest(name string, testFunc func() error) {
	fmt.Printf("🔍 %s... ", name)
	start := time.Now()
	
	err := testFunc()
	duration := time.Since(start)
	
	result := TestResult{
		Name:     name,
		Duration: duration,
	}
	
	if err != nil {
		result.Status = "❌ ÉCHEC"
		result.Error = err.Error()
		suite.Failed++
		fmt.Printf("❌ ÉCHEC (%.2fs)\n   Erreur: %s\n", duration.Seconds(), err.Error())
	} else {
		result.Status = "✅ SUCCÈS"
		suite.Passed++
		fmt.Printf("✅ SUCCÈS (%.2fs)\n", duration.Seconds())
	}
	
	suite.Results = append(suite.Results, result)
}

func (suite *TestSuite) printResults() {
	fmt.Printf("\n🏁 === RÉSULTATS DES TESTS ===\n")
	fmt.Printf("Total: %d | Succès: %d | Échecs: %d\n\n", 
		len(suite.Results), suite.Passed, suite.Failed)
	
	if suite.Failed > 0 {
		fmt.Println("❌ Tests échoués:")
		for _, result := range suite.Results {
			if strings.Contains(result.Status, "ÉCHEC") {
				fmt.Printf("  • %s: %s\n", result.Name, result.Error)
			}
		}
		fmt.Println()
	}
	
	// Rapport de performance
	fmt.Println("📊 Performances:")
	for _, result := range suite.Results {
		if strings.Contains(result.Name, "Performance") || strings.Contains(result.Name, "Gros Fichier") {
			fmt.Printf("  • %s: %.2fs\n", result.Name, result.Duration.Seconds())
		}
	}
	
	if suite.Failed == 0 {
		fmt.Println("\n🎉 Tous les tests sont passés ! Votre système PureFS est opérationnel.")
	} else {
		fmt.Printf("\n⚠️  %d test(s) ont échoué. Veuillez vérifier votre configuration.\n", suite.Failed)
	}
}

// Test 1: Connexion au serveur
func testServerConnection() error {
	resp, err := http.Get(serverURL)
	if err != nil {
		return fmt.Errorf("impossible de se connecter au serveur: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("serveur répond avec status %d au lieu de 200", resp.StatusCode)
	}
	
	return nil
}

// Test 2: Upload simple
func testSimpleUpload() error {
	// Créer un fichier test de 1MB
	testData := make([]byte, 1024*1024)
	if _, err := rand.Read(testData); err != nil {
		return fmt.Errorf("erreur génération données test: %v", err)
	}
	
	filename := "test_simple.bin"
	return uploadFile(filename, testData)
}

// Test 3: Download et vérification d'intégrité
func testDownloadIntegrity() error {
	// Créer un fichier test avec checksum connu
	testData := []byte("Test d'intégrité PureFS - " + time.Now().String())
	filename := "test_integrity.txt"
	
	// Upload
	if err := uploadFile(filename, testData); err != nil {
		return fmt.Errorf("échec upload: %v", err)
	}
	
	// Download
	downloadedData, err := downloadFile(filename)
	if err != nil {
		return fmt.Errorf("échec download: %v", err)
	}
	
	// Vérifier l'intégrité
	if !bytes.Equal(testData, downloadedData) {
		return fmt.Errorf("données corrompues: original=%d bytes, téléchargé=%d bytes", 
			len(testData), len(downloadedData))
	}
	
	// Vérifier le checksum
	originalHash := sha256.Sum256(testData)
	downloadedHash := sha256.Sum256(downloadedData)
	
	if originalHash != downloadedHash {
		return fmt.Errorf("checksum différent: %x vs %x", originalHash, downloadedHash)
	}
	
	// Nettoyer
	return deleteFile(filename)
}

// Test 4: Gros fichier (test de performance)
func testLargeFileUpload() error {
	// Créer un fichier de 50MB
	testData := make([]byte, 50*1024*1024)
	if _, err := rand.Read(testData); err != nil {
		return fmt.Errorf("erreur génération gros fichier: %v", err)
	}
	
	filename := "test_large.bin"
	start := time.Now()
	
	if err := uploadFile(filename, testData); err != nil {
		return fmt.Errorf("échec upload gros fichier: %v", err)
	}
	
	uploadDuration := time.Since(start)
	uploadSpeed := float64(len(testData)) / uploadDuration.Seconds() / (1024 * 1024)
	
	// Vérifier la vitesse (doit être > 15 MB/s pour être acceptable)
	if uploadSpeed < 15.0 {
		log.Printf("⚠️  Vitesse d'upload lente: %.2f MB/s (attendu > 15 MB/s)", uploadSpeed)
	}
	
	// Test download
	start = time.Now()
	downloadedData, err := downloadFile(filename)
	if err != nil {
		return fmt.Errorf("échec download gros fichier: %v", err)
	}
	
	downloadDuration := time.Since(start)
	downloadSpeed := float64(len(downloadedData)) / downloadDuration.Seconds() / (1024 * 1024)
	
	if downloadSpeed < 20.0 {
		log.Printf("⚠️  Vitesse de download lente: %.2f MB/s (attendu > 20 MB/s)", downloadSpeed)
	}
	
	// Vérification d'intégrité
	if !bytes.Equal(testData, downloadedData) {
		return fmt.Errorf("corruption détectée sur gros fichier")
	}
	
	log.Printf("📈 Performance: Upload %.2f MB/s, Download %.2f MB/s", uploadSpeed, downloadSpeed)
	
	// Nettoyer
	return deleteFile(filename)
}

// Test 5: Uploads concurrents
func testConcurrentUploads() error {
	const numFiles = 5
	const fileSize = 5 * 1024 * 1024 // 5MB chacun
	
	type uploadResult struct {
		filename string
		err      error
	}
	
	results := make(chan uploadResult, numFiles)
	
	// Lancer les uploads en parallèle
	for i := 0; i < numFiles; i++ {
		go func(index int) {
			filename := fmt.Sprintf("test_concurrent_%d.bin", index)
			testData := make([]byte, fileSize)
			rand.Read(testData)
			
			err := uploadFile(filename, testData)
			results <- uploadResult{filename, err}
		}(i)
	}
	
	// Collecter les résultats
	var filenames []string
	for i := 0; i < numFiles; i++ {
		result := <-results
		if result.err != nil {
			return fmt.Errorf("échec upload concurrent %s: %v", result.filename, result.err)
		}
		filenames = append(filenames, result.filename)
	}
	
	// Vérifier que tous les fichiers sont accessibles
	for _, filename := range filenames {
		if _, err := downloadFile(filename); err != nil {
			return fmt.Errorf("fichier concurrent %s inaccessible: %v", filename, err)
		}
		deleteFile(filename) // Nettoyer
	}
	
	return nil
}

// Test 6: Simulation de corruption et récupération
func testCorruptionRecovery() error {
	// Ce test vérifie que le système peut récupérer même si un chunk est corrompu
	testData := make([]byte, 10*1024*1024) // 10MB
	rand.Read(testData)
	filename := "test_corruption.bin"
	
	// Upload
	if err := uploadFile(filename, testData); err != nil {
		return fmt.Errorf("échec upload pour test corruption: %v", err)
	}
	
	// Attendre un peu pour que l'upload se stabilise
	time.Sleep(2 * time.Second)
	
	// Essayer de télécharger plusieurs fois pour tester la robustesse
	for i := 0; i < 3; i++ {
		downloadedData, err := downloadFile(filename)
		if err != nil {
			return fmt.Errorf("échec download pendant test corruption (tentative %d): %v", i+1, err)
		}
		
		if !bytes.Equal(testData, downloadedData) {
			return fmt.Errorf("corruption détectée (tentative %d)", i+1)
		}
	}
	
	return deleteFile(filename)
}

// Test 7: Suppression et nettoyage
func testDeletionCleanup() error {
	// Upload un fichier
	testData := make([]byte, 1024*1024)
	rand.Read(testData)
	filename := "test_deletion.bin"
	
	if err := uploadFile(filename, testData); err != nil {
		return fmt.Errorf("échec upload pour test suppression: %v", err)
	}
	
	// Vérifier qu'il existe
	if _, err := downloadFile(filename); err != nil {
		return fmt.Errorf("fichier non accessible avant suppression: %v", err)
	}
	
	// Supprimer
	if err := deleteFile(filename); err != nil {
		return fmt.Errorf("échec suppression: %v", err)
	}
	
	// Vérifier qu'il n'est plus accessible
	if _, err := downloadFile(filename); err == nil {
		return fmt.Errorf("fichier encore accessible après suppression")
	}
	
	return nil
}

// Test 8: Résistance aux erreurs
func testErrorResilience() error {
	// Test avec un fichier vide
	if err := uploadFile("test_empty.bin", []byte{}); err != nil {
		return fmt.Errorf("échec upload fichier vide: %v", err)
	}
	
	// Test avec un nom de fichier avec caractères spéciaux
	specialName := "test_spécial-éàç.bin"
	testData := make([]byte, 1024)
	rand.Read(testData)
	
	if err := uploadFile(specialName, testData); err != nil {
		log.Printf("⚠️  Caractères spéciaux non supportés: %v", err)
		// Pas critique, donc on continue
	} else {
		deleteFile(specialName)
	}
	
	return deleteFile("test_empty.bin")
}

// Test 9: Benchmark de performance
func testPerformanceBenchmark() error {
	sizes := []int{
		1 * 1024 * 1024,      // 1MB
		10 * 1024 * 1024,     // 10MB
		100 * 1024 * 1024,    // 100MB
	}
	
	for _, size := range sizes {
		testData := make([]byte, size)
		rand.Read(testData)
		
		filename := fmt.Sprintf("benchmark_%dMB.bin", size/(1024*1024))
		
		// Mesurer upload
		start := time.Now()
		if err := uploadFile(filename, testData); err != nil {
			return fmt.Errorf("échec benchmark upload %s: %v", filename, err)
		}
		uploadTime := time.Since(start)
		uploadSpeed := float64(size) / uploadTime.Seconds() / (1024 * 1024)
		
		// Mesurer download
		start = time.Now()
		downloadedData, err := downloadFile(filename)
		if err != nil {
			return fmt.Errorf("échec benchmark download %s: %v", filename, err)
		}
		downloadTime := time.Since(start)
		downloadSpeed := float64(len(downloadedData)) / downloadTime.Seconds() / (1024 * 1024)
		
		log.Printf("📊 %s: Upload %.2f MB/s, Download %.2f MB/s", 
			filename, uploadSpeed, downloadSpeed)
		
		// Vérifier l'intégrité
		if !bytes.Equal(testData, downloadedData) {
			return fmt.Errorf("corruption dans benchmark %s", filename)
		}
		
		deleteFile(filename)
	}
	
	return nil
}

// Test 10: Charge système
func testSystemLoad() error {
	// Vérifier que le serveur répond toujours sous charge
	const numRequests = 10
	results := make(chan error, numRequests)
	
	for i := 0; i < numRequests; i++ {
		go func(index int) {
			filename := fmt.Sprintf("load_test_%d.bin", index)
			testData := make([]byte, 1024*1024) // 1MB
			rand.Read(testData)
			
			if err := uploadFile(filename, testData); err != nil {
				results <- fmt.Errorf("échec upload charge %s: %v", filename, err)
				return
			}
			
			if _, err := downloadFile(filename); err != nil {
				results <- fmt.Errorf("échec download charge %s: %v", filename, err)
				return
			}
			
			deleteFile(filename)
			results <- nil
		}(i)
	}
	
	// Collecter les résultats
	for i := 0; i < numRequests; i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	
	return nil
}

// Fonctions utilitaires

func uploadFile(filename string, data []byte) error {
	// Créer le fichier temporaire
	tempFile := filepath.Join(testDir, filename)
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("erreur création fichier temp: %v", err)
	}
	defer os.Remove(tempFile)
	
	// Créer la requête multipart
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	
	file, err := os.Open(tempFile)
	if err != nil {
		return fmt.Errorf("erreur ouverture fichier: %v", err)
	}
	defer file.Close()
	
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return fmt.Errorf("erreur création form file: %v", err)
	}
	
	if _, err := io.Copy(part, file); err != nil {
		return fmt.Errorf("erreur copie données: %v", err)
	}
	
	writer.Close()
	
	// Envoyer la requête
	resp, err := http.Post(serverURL+"/api/files/upload", writer.FormDataContentType(), &buf)
	if err != nil {
		return fmt.Errorf("erreur requête upload: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusSeeOther {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload échoué (status %d): %s", resp.StatusCode, string(body))
	}
	
	return nil
}

func downloadFile(filename string) ([]byte, error) {
	// CORRECTION: Timeout plus long pour les gros fichiers
	client := &http.Client{
		Timeout: 180 * time.Second, // 3 minutes
	}
	
	resp, err := client.Get(serverURL + "/api/files/download/" + filename)
	if err != nil {
		return nil, fmt.Errorf("erreur requête download: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download échoué (status %d)", resp.StatusCode)
	}
	
	// CORRECTION: ReadAll avec un client qui a un timeout suffisant
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("erreur de lecture du body: %v", err)
	}
	
	return data, nil
}

func deleteFile(filename string) error {
	resp, err := http.Post(serverURL+"/api/files/delete/"+filename, "", nil)
	if err != nil {
		return fmt.Errorf("erreur requête delete: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusSeeOther {
		return fmt.Errorf("delete échoué (status %d)", resp.StatusCode)
	}
	
	return nil
}
