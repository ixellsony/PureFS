// --- disk_space_windows.go ---

//go:build windows

package main

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32           = syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceEx = kernel32.NewProc("GetDiskFreeSpaceExW")
)

// getAvailableSpace retourne l'espace libre en octets pour un chemin donné sous Windows.
func getAvailableSpace(path string) (uint64, error) {
	// S'assurer que le répertoire existe, car l'API Windows peut être imprévisible sinon.
	if err := os.MkdirAll(path, 0755); err != nil {
		return 0, fmt.Errorf("impossible de créer ou vérifier le répertoire de stockage %s: %w", path, err)
	}

	var freeBytesAvailable uint64
	var totalNumberOfBytes uint64
	var totalNumberOfFreeBytes uint64

	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}

	ret, _, err := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)

	// Si l'appel a échoué (ret==0), l'erreur système est dans `err`.
	if ret == 0 {
		return 0, err
	}

	return freeBytesAvailable, nil
}
