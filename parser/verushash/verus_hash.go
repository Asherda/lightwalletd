package verushash

import (
	"unsafe"
)

// Initialize verushash object once.
var verusHash = NewVerushash()

func VerusHash(serializedHeader []byte) []byte {
	hash := make([]byte, 32)
	ptrHash := uintptr(unsafe.Pointer(&hash[0]))
	length := len(serializedHeader)
	verusHash.Verushash(string(serializedHeader), length, ptrHash)
	return hash
}

func VerusHash_V2B(serializedHeader []byte) []byte {
	hash := make([]byte, 32)
	ptrHash := uintptr(unsafe.Pointer(&hash[0]))
	length := len(serializedHeader)
	verusHash.Verushash_v2b(string(serializedHeader), length, ptrHash)
	return hash
}

func VerusHash_V2B1(serializedHeader []byte) []byte {
	hash := make([]byte, 32)
	ptrHash := uintptr(unsafe.Pointer(&hash[0]))
	length := len(serializedHeader)
	verusHash.Verushash_v2b1(string(serializedHeader), length, ptrHash)
	return hash
}

func HashHeader(serializedHeader []byte) []byte {
	hash := make([]byte, 32)
	ptrHash := uintptr(unsafe.Pointer(&hash[0]))
	if !verusHash.Get_verus_v2_hash(string(serializedHeader), ptrHash) {
		return nil
	}
	return hash
}
