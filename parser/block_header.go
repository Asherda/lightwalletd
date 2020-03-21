package parser

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/big"

	"github.com/asherda/lightwalletd/parser/internal/bytestring"
	"github.com/pkg/errors"
)

//#cgo CPPFLAGS: -O2 -march=x86-64 -msse4 -msse2 -msse -msse4.1 -msse4.2 -msse3 -mavx -maes -fomit-frame-pointer -fPIC -Wno-builtin-declaration-mismatch -I/home/virtualsoundnw/lightwalletd/parser -I/home/virtualsoundnw/lightwalletd/parser/bitcoin/src -I/usr/include/c++/8-I/usr/include/x86_64-linux-gnu/c++/8 -I/home/virtualsoundnw/lightwalletd/parser/bitcoin/src  -pthread -w
//#cgo CXXFLAGS: -O2 -march=x86-64 -msse2 -msse -msse4 -msse4.1 -msse4.2 -msse3 -mavx -maes -fomit-frame-pointer -fPIC -Wno-builtin-declaration-mismatch -I/home/virtualsoundnw/lightwalletd/parser -I/home/virtualsoundnw/lightwalletd/parser/bitcoin/src -I/usr/include/c++/8 -I/usr/include/x86_64-linux-gnu/c++/8 -I/home/virtualsoundnw/lightwalletd/parser/bitcoin/src  -pthread -w
//#cgo CFLAGS: -O2 -march=x86-64 -msse2 -msse -msse4 -msse4.1 -msse4.2 -msse3 -mavx -maes -fomit-frame-pointer -fPIC -Wno-builtin-declaration-mismatch -I/home/virtualsoundnw/lightwalletd/parser -I/home/virtualsoundnw/lightwalletd/parser/bitcoin/src -I/usr/include/c++/8 -I/usr/include/x86_64-linux-gnu/c++/8 -I/home/virtualsoundnw/lightwalletd/parser/bitcoin/src  -pthread -w
//#cgo LDFLAGS: -L. -l libverus_crypto
//char *  wrapVerushash(char * s)
//{
//  char * hash = verushash(s);
//  return hash;
//}
//char *  wrapVerushash_v2(char * s)
//{
//  char * hash = verushash_v2(s);
//  return hash;
//}
//char *  wrapVerushash_v2b(char * s)
//{
//  char * hash = verushash_v2b(s);
//  return hash;
//}
//char *  wrapVerushash_v2b1(char * s)
//{
//  char * hash = verushash_v2b1(s);
//  return hash;
//}
import "C"

const (
	serBlockHeaderMinusEquihashSize = 140  // size of a serialized block header minus the Equihash solution
	equihashSizeMainnet             = 1344 // size of a mainnet / testnet Equihash solution in bytes
)

// A block header as defined in version 2018.0-beta-29 of the Zcash Protocol Spec.
type rawBlockHeader struct {
	// The block version number indicates which set of block validation rules
	// to follow. The current and only defined block version number for Zcash
	// is 4.
	Version int32

	// A SHA-256d hash in internal byte order of the previous block's header. This
	// ensures no previous block can be changed without also changing this block's
	// header.
	HashPrevBlock []byte

	// A SHA-256d hash in internal byte order. The merkle root is derived from
	// the hashes of all transactions included in this block, ensuring that
	// none of those transactions can be modified without modifying the header.
	HashMerkleRoot []byte

	// [Pre-Sapling] A reserved field which should be ignored.
	// [Sapling onward] The root LEBS2OSP_256(rt) of the Sapling note
	// commitment tree corresponding to the final Sapling treestate of this
	// block.
	HashFinalSaplingRoot []byte

	// The block time is a Unix epoch time (UTC) when the miner started hashing
	// the header (according to the miner).
	Time uint32

	// An encoded version of the target threshold this block's header hash must
	// be less than or equal to, in the same nBits format used by Bitcoin.
	NBitsBytes []byte

	// An arbitrary field that miners can change to modify the header hash in
	// order to produce a hash less than or equal to the target threshold.
	Nonce []byte

	// The Equihash solution. In the wire format, this is a
	// CompactSize-prefixed value.
	Solution []byte
}

type BlockHeader struct {
	*rawBlockHeader
	cachedHash      []byte
	targetThreshold *big.Int
}

// CompactLengthPrefixedLen calculates the total number of bytes needed to
// encode 'length' bytes.
func CompactLengthPrefixedLen(length int) int {
	if length < 253 {
		return 1 + length
	} else if length <= 0xffff {
		return 1 + 2 + length
	} else if length <= 0xffffffff {
		return 1 + 4 + length
	} else {
		return 1 + 8 + length
	}
}

// WriteCompactLengthPrefixedLen writes the given length to the stream.
func WriteCompactLengthPrefixedLen(buf *bytes.Buffer, length int) {
	if length < 253 {
		binary.Write(buf, binary.LittleEndian, uint8(length))
	} else if length <= 0xffff {
		binary.Write(buf, binary.LittleEndian, byte(253))
		binary.Write(buf, binary.LittleEndian, uint16(length))
	} else if length <= 0xffffffff {
		binary.Write(buf, binary.LittleEndian, byte(254))
		binary.Write(buf, binary.LittleEndian, uint32(length))
	} else {
		binary.Write(buf, binary.LittleEndian, byte(255))
		binary.Write(buf, binary.LittleEndian, uint64(length))
	}
}

func WriteCompactLengthPrefixed(buf *bytes.Buffer, val []byte) {
	WriteCompactLengthPrefixedLen(buf, len(val))
	binary.Write(buf, binary.LittleEndian, val)
}

func (hdr *rawBlockHeader) GetSize() int {
	return serBlockHeaderMinusEquihashSize + CompactLengthPrefixedLen(len(hdr.Solution))
}

func (hdr *rawBlockHeader) MarshalBinary() ([]byte, error) {
	headerSize := hdr.GetSize()
	backing := make([]byte, 0, headerSize)
	buf := bytes.NewBuffer(backing)
	binary.Write(buf, binary.LittleEndian, hdr.Version)
	binary.Write(buf, binary.LittleEndian, hdr.HashPrevBlock)
	binary.Write(buf, binary.LittleEndian, hdr.HashMerkleRoot)
	binary.Write(buf, binary.LittleEndian, hdr.HashFinalSaplingRoot)
	binary.Write(buf, binary.LittleEndian, hdr.Time)
	binary.Write(buf, binary.LittleEndian, hdr.NBitsBytes)
	binary.Write(buf, binary.LittleEndian, hdr.Nonce)
	WriteCompactLengthPrefixed(buf, hdr.Solution)
	return backing[:headerSize], nil
}

func NewBlockHeader() *BlockHeader {
	return &BlockHeader{
		rawBlockHeader: new(rawBlockHeader),
	}
}

// ParseFromSlice parses the block header struct from the provided byte slice,
// advancing over the bytes read. If successful it returns the rest of the
// slice, otherwise it returns the input slice unaltered along with an error.
func (hdr *BlockHeader) ParseFromSlice(in []byte) (rest []byte, err error) {
	s := bytestring.String(in)

	// Primary parsing layer: sort the bytes into things

	if !s.ReadInt32(&hdr.Version) {
		return in, errors.New("could not read header version")
	}

	if !s.ReadBytes(&hdr.HashPrevBlock, 32) {
		return in, errors.New("could not read HashPrevBlock")
	}

	if !s.ReadBytes(&hdr.HashMerkleRoot, 32) {
		return in, errors.New("could not read HashMerkleRoot")
	}

	if !s.ReadBytes(&hdr.HashFinalSaplingRoot, 32) {
		return in, errors.New("could not read HashFinalSaplingRoot")
	}

	if !s.ReadUint32(&hdr.Time) {
		return in, errors.New("could not read timestamp")
	}

	if !s.ReadBytes(&hdr.NBitsBytes, 4) {
		return in, errors.New("could not read NBits bytes")
	}

	if !s.ReadBytes(&hdr.Nonce, 32) {
		return in, errors.New("could not read Nonce bytes")
	}

	if !s.ReadCompactLengthPrefixed((*bytestring.String)(&hdr.Solution)) {
		return in, errors.New("could not read CompactSize-prefixed Equihash solution")
	}

	// TODO: interpret the bytes
	//hdr.targetThreshold = parseNBits(hdr.NBitsBytes)

	return []byte(s), nil
}

func parseNBits(b []byte) *big.Int {
	byteLen := int(b[0])

	targetBytes := make([]byte, byteLen)
	copy(targetBytes, b[1:])

	// If high bit set, return a negative result. This is in the Bitcoin Core
	// test vectors even though Bitcoin itself will never produce or interpret
	// a difficulty lower than zero.
	if b[1]&0x80 != 0 {
		targetBytes[0] &= 0x7F
		target := new(big.Int).SetBytes(targetBytes)
		target.Neg(target)
		return target
	}

	return new(big.Int).SetBytes(targetBytes)
}

// GetDisplayHash returns the bytes of a block hash in big-endian order.
func (hdr *BlockHeader) GetDisplayHash() []byte {
	if hdr.cachedHash != nil {
		return hdr.cachedHash
	}

	serializedHeader, err := hdr.MarshalBinary()
	if err != nil {
		log.Fatalf("error marshaling block header: %v", err)
		return nil
	}

    digest = C.wrapVerushash(serializedHeader)

	// Reverse byte order
	for i := 0; i < len(digest)/2; i++ {
		j := len(digest) - 1 - i
		digest[i], digest[j] = digest[j], digest[i]
	}

	hdr.cachedHash = digest[:]
	return hdr.cachedHash
}

// GetEncodableHash returns the bytes of a block hash in little-endian wire order.
func (hdr *BlockHeader) GetEncodableHash() []byte {
	serializedHeader, err := hdr.MarshalBinary()

	if err != nil {
		log.Fatalf("error marshaling block header: %v", err)
		return nil
	}

    digest = C.wrapVerushash(serializedHeader)
	return digest[:]
}

func (hdr *BlockHeader) GetDisplayPrevHash() []byte {
	rhash := make([]byte, len(hdr.HashPrevBlock))
	copy(rhash, hdr.HashPrevBlock)
	// Reverse byte order
	for i := 0; i < len(rhash)/2; i++ {
		j := len(rhash) - 1 - i
		rhash[i], rhash[j] = rhash[j], rhash[i]
	}
	return rhash
}
