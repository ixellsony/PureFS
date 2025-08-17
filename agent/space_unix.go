//go:build unix

package main

import (
	"syscall"
)

// getAvailableSpace retourne l'espace libre disponible sur le système de fichiers
// contenant le chemin spécifié (Unix/Linux)
func getAvailableSpace(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	
	// Calcul de l'espace libre disponible
	// Bavail = blocs disponibles pour les utilisateurs non-root
	// Bsize = taille d'un bloc en bytes
	freeBytes := uint64(stat.Bavail) * uint64(stat.Bsize)
	
	return freeBytes, nil
}
