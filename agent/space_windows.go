//go:build windows

package main

import (
	"syscall"
	"unsafe"
)

var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceEx = kernel32.NewProc("GetDiskFreeSpaceExW")
)

// getAvailableSpace retourne l'espace libre disponible sur le système de fichiers
// contenant le chemin spécifié (Windows)
func getAvailableSpace(path string) (uint64, error) {
	var freeBytes uint64
	var totalBytes uint64
	
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	
	ret, _, err := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytes)),
		uintptr(unsafe.Pointer(&totalBytes)),
		0,
	)
	
	if ret == 0 {
		return 0, err
	}
	
	return freeBytes, nil
}
