// --- disk_space_unix.go ---

//go:build linux || darwin || freebsd

package main

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// getAvailableSpace retourne l'espace libre en octets pour un chemin donné sous Unix.
func getAvailableSpace(path string) (uint64, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return 0, fmt.Errorf("impossible de créer ou vérifier le répertoire de stockage %s: %w", path, err)
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("impossible d'obtenir les statistiques du système de fichiers pour %s: %w", path, err)
	}
	// Blocs disponibles * taille par bloc = espace total disponible en octets
	return stat.Bavail * uint64(stat.Bsize), nil
}
