// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .

// Package common contains utilities that are shared by other packages.
package common

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"strconv"
	"sync"

	"github.com/asherda/lightwalletd/walletrpc"
	"github.com/golang/protobuf/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	blockHeightPrefix = "B" // key is "B" + block height, value is block; see also H, block by hash
	blockHashPrefix   = "H" // key is "H" + block hash, value is block; see also B, block by height
	idPrefix          = "I" // key is "I" + chain ID, value is height (more to come), see next (verusID)
	saplingTreePrefix = "S" // key is "S", value is sapling tree size by height
)

// BlockCache contains a consecutive set of recent compact blocks in marshalled form.
type BlockCache struct {
	verusID    string
	firstBlock int         // height of the first block in the cache (we start at 1)
	nextBlock  int         // height of the first block not in the cache
	latestHash []byte      // hash of the most recent (highest height) block, for detecting reorgs.
	ldb        *leveldb.DB // levelDB connection
	mutex      sync.RWMutex
}

// GetNextHeight returns the height of the lowest unobtained block.
func (c *BlockCache) GetNextHeight() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.nextBlock
}

// GetFirstHeight returns the height of the lowest block (usually Sapling activation).
func (c *BlockCache) GetFirstHeight() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.firstBlock
}

// GetLatestHash returns the hash (block ID) of the most recent (highest) known block.
func (c *BlockCache) GetLatestHash() []byte {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.latestHash
}

// HashMatch indicates if the given prev-hash matches the most recent block's hash
// so reorgs can be detected.
func (c *BlockCache) HashMatch(prevhash []byte) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.latestHash == nil || bytes.Equal(c.latestHash, prevhash)
}

// Make the block at the given height the lowest height that we don't have.
// In other words, wipe out this height and beyond.
// This should never increase the size of the cache, only decrease.
// Caller should hold c.mutex.Lock().
func (c *BlockCache) setDbHeight(height int) {
	if height <= c.nextBlock {
		if height < c.firstBlock {
			height = c.firstBlock
		}
		c.flushBlocks(height, c.nextBlock)
		c.Sync()
		c.storeNewHeight(true)
		c.nextBlock = height
		c.setLatestHash()
	}
}

// Caller should hold c.mutex.Lock().
func (c *BlockCache) recoverFromCorruption(height int) {
	c.flushBlocks(height, c.nextBlock)
	Log.Warning("CORRUPTION detected in db blocks-cache files, height ", height, " redownloading")
	c.setDbHeight(height)
}

// Calculate the 8-byte checksum that precedes each block in the blocks records.
func checksum(height int, b []byte) []byte {
	h := make([]byte, 8)
	binary.LittleEndian.PutUint64(h, uint64(height))
	cs := fnv.New64a()
	cs.Write(h)
	cs.Write(b)
	return cs.Sum(nil)
}

// Caller should hold (at least) c.mutex.RLock().
func (c *BlockCache) readBlock(height int) *walletrpc.CompactBlock {
	if c.ldb == nil {
		return nil
	}

	cacheResult, err := c.ldb.Get([]byte(blockHeightPrefix+strconv.Itoa(height)), nil)
	if err != nil {
		return nil
	}
	if len(cacheResult) < 72 {
		Log.Warning("block read height: ", height, " failed, result too short. ")
		return nil
	}

	cachecs := cacheResult[:8]
	b := cacheResult[8:]
	if !bytes.Equal(checksum(height, b), cachecs) {
		Log.Warning("bad block checksum at height: ", height)
		return nil
	}
	block := &walletrpc.CompactBlock{}
	err = proto.Unmarshal(b, block)
	if err != nil {
		// Could be file corruption.
		Log.Warning("blocks unmarshal at height: ", height, " failed: ", err)
		return nil
	}
	if int(block.Height) != height {
		// Could be file corruption.
		Log.Warning("block unexpected height at height ", height)
		return nil
	}
	return block
}

// Caller should hold c.mutex.Lock().
func (c *BlockCache) setLatestHash() {
	c.latestHash = nil
	// There is at least one block; get the last block's hash
	if c.nextBlock > c.firstBlock {
		// At least one block remains; get the last block's hash
		block := c.readBlock(c.nextBlock - 1)
		if block == nil {
			c.recoverFromCorruption(c.nextBlock - 10000)
			return
		}
		c.latestHash = make([]byte, len(block.Hash))
		copy(c.latestHash, block.Hash)
	}
}

// Reset is used only for darkside testing.
func (c *BlockCache) Reset(startHeight int) {
	c.setDbHeight(c.firstBlock) // empty the cache
	c.firstBlock = startHeight
	c.nextBlock = startHeight
}

// NewBlockCache returns an instance of a block cache object.
// (No locking here, we assume this is single-threaded.)
// Currently this is a startup only task, so it is indeed single threaded.
//
// Multichain may go to per chain DB, so each cache has a levelDB connection
// for it's own DB & we can do multiple chains in a single lwd easily.
func NewBlockCache(db *leveldb.DB, chainID string, startHeight int, redownload bool) *BlockCache {
	c := &BlockCache{}
	c.verusID = chainID
	c.ldb = db
	c.firstBlock = startHeight

	// Fetch the cache highwater record for the VerusCoin chain cache
	// H prefix for height
	data, err := c.ldb.Get([]byte(idPrefix+c.verusID), nil)
	if err != nil {
		Log.Warning("No max cache height record, starting with no cache", err)
		c.nextBlock = c.firstBlock
		if c.storeNewHeight(false) != nil {
			Log.Fatal("Unable to record new (reset) high water mark: ", c.nextBlock)
		}
	} else {
		c.nextBlock = int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24 |
			int(data[4])<<32 | int(data[5])<<40 | int(data[6])<<48 | int(data[7])<<56
	}
	if redownload {
		c.flushBlocks(c.firstBlock, c.nextBlock)
	}

	for i := c.firstBlock; i < c.nextBlock; i++ {

		// Check for corruption.
		block := c.readBlock(i)
		if block == nil {
			Log.Warning("error, record not found reading block at height ", i, ", attempting to recover")
			c.recoverFromCorruption(c.nextBlock)
			break
		}
	}
	Log.Info("Found ", c.nextBlock-c.firstBlock, " blocks in cache")
	return c
}

// Add adds the given block to the cache at the given height, returning true
// if a reorg was detected.
func (c *BlockCache) Add(height int, block *walletrpc.CompactBlock) error {
	// Invariant: m[firstBlock..nextBlock) are valid.
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if height > c.nextBlock {
		// Cache has been reset (for example, checksum error)
		return nil
	}
	if height < c.firstBlock {
		// Should never try to add a block before Sapling activation height
		Log.Fatal("cache.Add height below Sapling: ", height)
		return nil
	}
	if height < c.nextBlock {
		// Should never try to "backup" (call Reorg() instead).
		Log.Fatal("cache.Add height going backwards: ", height)
		return nil
	}

	if int(block.Height) != height {
		Log.Fatal("cache.Add wrong height: ", block.Height, " expecting: ", height)
		return nil
	}

	prevSize := uint64(0)
	if height > c.firstBlock {
		prevSize = c.getSaplingTreeSize(height - 1)
	}

	var blockSize uint64
	for _, tx := range block.Vtx {
		blockSize += uint64(len(tx.Outputs))
	}

	newSize := prevSize + blockSize

	block.ChainMetadata = &walletrpc.ChainMetadata{
		SaplingCommitmentTreeSize: &newSize,
	}

	if height > c.firstBlock {
		prevSize := c.getSaplingTreeSize(height - 1)
		if newSize < prevSize {
			// should always increase or stay the same on inter-block basis
			Log.Fatal("sapling tree size decreased at height ", height)
		}
	}

	if err := c.storeSaplingTreeSize(height, newSize); err != nil {
		Log.Fatal("failed to store sapling tree size at height ", height, ": ", err)
	}

	data, err := proto.Marshal(block)
	if err != nil {
		return err
	}

	checkSummed := checksum(height, data)
	checkSummed = append(checkSummed, data...)

	if err := c.storeNewBlock(height, checkSummed); err != nil {
		Log.Fatal("hash write failed at height", height, ": ", err)
	}

	if c.latestHash == nil {
		c.latestHash = make([]byte, len(block.Hash))
	}
	copy(c.latestHash, block.Hash)

	c.nextBlock++
	// Invariant: m[firstBlock..nextBlock) are valid.

	if err := c.storeNewHeight(false); err != nil {
		Log.Fatal("height write failed at height", height, ": ", err)
	}
	return nil
}

// Reorg resets nextBlock (the block that should be Add()ed next)
// downward to the given height.
func (c *BlockCache) Reorg(height int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Allow the caller not to have to worry about Sapling start height.
	if height < c.firstBlock {
		height = c.firstBlock
	}
	if height >= c.nextBlock {
		// Timing window, ignore this request
		return
	}
	// Remove the end of the cache.
	c.flushBlocks(height+1, c.nextBlock)

	// adjust to the new height
	c.nextBlock = height
	c.storeNewHeight(true)
	c.setLatestHash()
}

// Get returns the compact block at the requested height if it's
// in the cache, else nil.
func (c *BlockCache) Get(height int) *walletrpc.CompactBlock {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if height < c.firstBlock || height >= c.nextBlock {
		return nil
	}
	block := c.readBlock(height)
	if block == nil {
		go func() {
			// We hold only the read lock, need the exclusive lock.
			c.mutex.Lock()
			c.recoverFromCorruption(height - 10000)
			c.mutex.Unlock()
		}()
		return nil
	}
	return block
}

// GetLatestHeight returns the height of the most recent block, or -1
// if the cache is empty.
func (c *BlockCache) GetLatestHeight() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.firstBlock == c.nextBlock {
		return -1
	}
	return c.nextBlock - 1
}

// Sync ensures that the db files are flushed to disk, can be called unnecessarily.
func (c *BlockCache) Sync() {
	c.storeNewHeight(true)
}

// Close is Currently used only for testing.
func (c *BlockCache) Close() {
	// Some operating system require you to close files before you can remove them.
	if c.ldb != nil {
		c.ldb.Close()
	}
}

func (c *BlockCache) flushBlocks(height int, last int) {
	for i := height; i < last; i++ {
		c.flushBlock(i)
	}
	c.nextBlock = height
	c.storeNewHeight(true)
}

func (c *BlockCache) flushBlock(height int) {
	key := []byte(blockHeightPrefix + strconv.Itoa(height))
	err := c.ldb.Delete(key, &opt.WriteOptions{Sync: false})
	if err != nil {
		Log.Warning("error flushing block (by height) at height ", height, ": ", err)
	}

	if c.latestHash != nil {
		hashID := append([]byte(blockHashPrefix), c.latestHash...)
		err = c.ldb.Delete(hashID, &opt.WriteOptions{Sync: false})
		if err != nil {
			Log.Warning("error flushing block (by hash) at height ", height, ": ", err)
		}
	}

	treeKey := []byte(saplingTreePrefix + strconv.Itoa(height))
	err = c.ldb.Delete(treeKey, &opt.WriteOptions{Sync: false})
	if err != nil {
		Log.Warning("error flushing sapling tree size at height ", height, ": ", err)
	}
}

func (c *BlockCache) storeNewHeight(sync bool) error {
	bytesHeight := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytesHeight, (uint64)(c.nextBlock&0xFFFFFFFFFFFFFFF))
	return c.ldb.Put([]byte(idPrefix+c.verusID), bytesHeight, &opt.WriteOptions{Sync: sync})
}

func (c *BlockCache) storeNewBlock(height int, block []byte) error {
	err := c.ldb.Put([]byte(blockHeightPrefix+strconv.Itoa(height)), block, &opt.WriteOptions{Sync: false})
	if err != nil {
		Log.Fatal("blocks write at height", height, "failed: ", err)
		return err
	}
	var hashID []byte = nil
	copy(hashID, blockHashPrefix)
	hashID = append(hashID, []byte(c.latestHash)...)
	err = c.ldb.Put(hashID, block, &opt.WriteOptions{Sync: false})
	if err != nil {
		Log.Fatal("hash write at height", height, "failed: ", err)
		return err
	}
	return nil
}

// storeSaplingTreeSize stores cumulative tree size at a given height
func (c *BlockCache) storeSaplingTreeSize(height int, size uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, size)
	return c.ldb.Put([]byte(saplingTreePrefix+strconv.Itoa(height)), buf, &opt.WriteOptions{Sync: false})
}

// getSaplingTreeSize retrieves cumulative tree size at height
func (c *BlockCache) getSaplingTreeSize(height int) uint64 {
	data, err := c.ldb.Get([]byte(saplingTreePrefix+strconv.Itoa(height)), nil)
	if err != nil || len(data) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(data)
}
