// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .
package common

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/asherda/lightwalletd/parser"
	"github.com/asherda/lightwalletd/walletrpc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ------------------------------------------ Setup
//
// This section does some setup things that may (even if not currently)
// be useful across multiple tests.

var (
	testT *testing.T

	// The various stub callbacks need to sequence through states
	step int

	getblockchaininfoReply []byte
	logger                 = logrus.New()

	blocks [][]byte // four test blocks

	testcache *BlockCache
)

// TestMain does common setup that's shared across multiple tests
func TestMain(m *testing.M) {
	output, err := os.OpenFile("test-log", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Cannot open test-log: %v", err))
		os.Exit(1)
	}
	logger.SetOutput(output)
	Log = logger.WithFields(logrus.Fields{
		"app": "test",
	})

	// Several tests need test blocks; read all 4 into memory just once
	// (for efficiency).
	testBlocks, err := os.Open("../testdata/blocks")
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Cannot open testdata/blocks: %v", err))
		os.Exit(1)
	}
	scan := bufio.NewScanner(testBlocks)
	for scan.Scan() { // each line (block)
		blockJSON, _ := json.Marshal(scan.Text())
		blocks = append(blocks, blockJSON)
	}
	db, err := leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Cannot open test cache db: %v", err))
		os.Exit(1)
	}
	testcache = NewBlockCache(db, unitTestChain, 380640, true)

	// Setup is done; run all tests.
	exitcode := m.Run()

	// cleanup
	os.Remove("test-log")

	os.Exit(exitcode)
}

// Allow tests to verify that sleep has been called (for retries)
var sleepCount int
var sleepDuration time.Duration

func sleepStub(d time.Duration) {
	sleepCount++
	sleepDuration += d
}
func nowStub() time.Time {
	start := time.Time{}
	return start.Add(sleepDuration)
}

// afterStub returns a pre-fired channel so the select case in GetMempool's
// cancel-aware sleep fires immediately, accumulating sleepDuration like
// sleepStub does. Used by tests that exercise GetMempool's wait loop.
func afterStub(d time.Duration) <-chan time.Time {
	sleepCount++
	sleepDuration += d
	ch := make(chan time.Time, 1)
	ch <- time.Time{}
	return ch
}

// ------------------------------------------ GetLightdInfo()

func getLightdInfoStub(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
	step++
	switch method {
	case "getinfo":
		r, _ := json.Marshal(&ZcashdRpcReplyGetinfo{})
		return r, nil

	case "getblockchaininfo":
		// Test retry logic (for the moment, it's very simple, just one retry).
		switch step {
		case 1:
			return json.RawMessage{}, errors.New("first failure")
		case 2:
			if sleepCount != 1 || sleepDuration != 15*time.Second {
				testT.Error("unexpected sleeps", sleepCount, sleepDuration)
			}
		}
		// GetLightdInfo reports ChainName from `name`.
		r, _ := json.Marshal(&ZcashdRpcReplyGetblockchaininfo{
			Blocks:    9977,
			Name:      "bugsbunny",
			Consensus: ConsensusInfo{Chaintip: "someid"},
		})
		return r, nil
	}
	return nil, nil
}

func TestGetLightdInfo(t *testing.T) {
	testT = t
	RawRequest = getLightdInfoStub
	Time.Sleep = sleepStub
	// This calls the getblockchaininfo rpc just to establish connectivity with zcashd
	FirstRPC()

	// Ensure the retry happened as expected
	logFile, err := ioutil.ReadFile("test-log")
	if err != nil {
		t.Fatal("Cannot read test-log", err)
	}
	logStr := string(logFile)
	if !strings.Contains(logStr, "retrying") {
		t.Fatal("Cannot find retrying in test-log")
	}
	if !strings.Contains(logStr, "retry=1") {
		t.Fatal("Cannot find retry=1 in test-log")
	}

	// Check the success case (second attempt)
	getLightdInfo, err := GetLightdInfo()
	if err != nil {
		t.Fatal("GetLightdInfo failed")
	}
	if getLightdInfo.SaplingActivationHeight != 0 {
		t.Error("unexpected saplingActivationHeight", getLightdInfo.SaplingActivationHeight)
	}
	if getLightdInfo.BlockHeight != 9977 {
		t.Error("unexpected blockHeight", getLightdInfo.BlockHeight)
	}
	if getLightdInfo.ChainName != "bugsbunny" {
		t.Error("unexpected chainName", getLightdInfo.ChainName)
	}
	if getLightdInfo.ConsensusBranchId != "someid" {
		t.Error("unexpected ConsensusBranchId", getLightdInfo.ConsensusBranchId)
	}

	if sleepCount != 1 || sleepDuration != 15*time.Second {
		t.Error("unexpected sleeps", sleepCount, sleepDuration)
	}
	step = 0
	sleepCount = 0
	sleepDuration = 0
}

// ------------------------------------------ BlockIngestor()

func checkSleepMethod(count int, duration time.Duration, expected string, method string) {
	if sleepCount != count {
		testT.Fatal("unexpected sleep count")
	}
	if sleepDuration != duration*time.Second {
		testT.Fatal("unexpected sleep duration")
	}
	if method != expected {
		testT.Error("unexpected method")
	}
}

// There are four test blocks, 0..3
func blockIngestorStub(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
	step++
	// request the first two blocks very quickly (syncing),
	// then next block isn't yet available
	switch step {
	case 1:
		checkSleepMethod(0, 0, "getbestblockhash", method)
		// This hash doesn't matter, won't match anything
		r, _ := json.Marshal("010101")
		return r, nil
	case 2:
		checkSleepMethod(0, 0, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380640" {
			testT.Fatal("incorrect height requested")
		}
		// height 380640
		return blocks[0], nil
	case 3:
		checkSleepMethod(0, 0, "getbestblockhash", method)
		// This hash doesn't matter, won't match anything
		r, _ := json.Marshal("010101")
		return r, nil
	case 4:
		checkSleepMethod(0, 0, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380641" {
			testT.Fatal("incorrect height requested")
		}
		// height 380641
		return blocks[1], nil
	case 5:
		// Return the expected block hash, so we're synced, should
		// then sleep for 2 seconds, then another getbestblockhash
		checkSleepMethod(0, 0, "getbestblockhash", method)
		r, _ := json.Marshal(displayHash(testcache.GetLatestHash()))
		return r, nil
	case 6:
		// Simulate still no new block, still synced, should
		// sleep for 2 seconds, then another getbestblockhash
		checkSleepMethod(1, 2, "getbestblockhash", method)
		r, _ := json.Marshal(displayHash(testcache.GetLatestHash()))
		return r, nil
	case 7:
		// Simulate new block (any non-matching hash will do)
		checkSleepMethod(2, 4, "getbestblockhash", method)
		r, _ := json.Marshal("aabb")
		return r, nil
	case 8:
		checkSleepMethod(2, 4, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380642" {
			testT.Fatal("incorrect height requested")
		}
		// height 380642
		return blocks[2], nil
	case 9:
		// Simulate still no new block, still synced, should
		// sleep for 2 seconds, then another getbestblockhash
		checkSleepMethod(2, 4, "getbestblockhash", method)
		r, _ := json.Marshal(displayHash(testcache.GetLatestHash()))
		return r, nil
	case 10:
		// There are 3 blocks in the cache (380640-642), so let's
		// simulate a 1-block reorg, new version (replacement) of 380642
		checkSleepMethod(3, 6, "getbestblockhash", method)
		// hash doesn't matter, just something that doesn't match
		r, _ := json.Marshal("4545")
		return r, nil
	case 11:
		// It thinks there may simply be a new block, but we'll say
		// there is no block at this height (380642 was replaced).
		checkSleepMethod(3, 6, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380643" {
			testT.Fatal("incorrect height requested")
		}
		return nil, errors.New("-8: Block height out of range")
	case 12:
		// It will re-ask the best hash (let's make no change)
		checkSleepMethod(3, 6, "getbestblockhash", method)
		// hash doesn't matter, just something that doesn't match
		r, _ := json.Marshal("4545")
		return r, nil
	case 13:
		// It should have backed up one block
		checkSleepMethod(3, 6, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380642" {
			testT.Fatal("incorrect height requested")
		}
		// height 380642
		return blocks[2], nil
	case 14:
		// We're back to the same state as case 9, and this time
		// we'll make it back up 2 blocks (rather than one)
		checkSleepMethod(3, 6, "getbestblockhash", method) // XXXXXXXXXXXXXXXXXXXXXXXXXXXXX XXX
		// hash doesn't matter, just something that doesn't match
		r, _ := json.Marshal("5656")
		return r, nil
	case 15:
		// It thinks there may simply be a new block, but we'll say
		// there is no block at this height (380642 was replaced).
		checkSleepMethod(3, 6, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380643" {
			testT.Fatal("incorrect height requested")
		}
		return nil, errors.New("-8: Block height out of range")
	case 16:
		checkSleepMethod(3, 6, "getbestblockhash", method)
		// hash doesn't matter, just something that doesn't match
		r, _ := json.Marshal("5656")
		return r, nil
	case 17:
		// Like case 13, it should have backed up one block, but
		// this time we'll make it back up one more
		checkSleepMethod(3, 6, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380642" {
			testT.Fatal("incorrect height requested")
		}
		return nil, errors.New("-8: Block height out of range")
	case 18:
		checkSleepMethod(3, 6, "getbestblockhash", method)
		// hash doesn't matter, just something that doesn't match
		r, _ := json.Marshal("5656")
		return r, nil
	case 19:
		// It should have backed up one more
		checkSleepMethod(3, 6, "getblock", method)
		var height string
		err := json.Unmarshal(params[0], &height)
		if err != nil {
			testT.Fatal("could not unmarshal height")
		}
		if height != "380641" {
			testT.Fatal("incorrect height requested")
		}
		return blocks[1], nil
	}
	testT.Error("blockIngestorStub called too many times")
	return nil, nil
}

func TestBlockIngestor(t *testing.T) {
	testT = t
	RawRequest = blockIngestorStub
	Time.Sleep = sleepStub
	Time.Now = nowStub
	testcache = NewBlockCache(testCacheDB(t), unitTestChain, 380640, false)
	BlockIngestor(testcache, 11)
	if step != 19 {
		t.Error("unexpected final step", step)
	}
	step = 0
	sleepCount = 0
	sleepDuration = 0
}

// ------------------------------------------ GetBlockRange()

// There are four test blocks, 0..3
// (probably don't need all these cases)
func getblockStub(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
	if method != "getblock" {
		testT.Error("unexpected method")
	}
	var height string
	err := json.Unmarshal(params[0], &height)
	if err != nil {
		testT.Fatal("could not unmarshal height")
	}

	step++
	switch step {
	case 1:
		if height != "380640" {
			testT.Error("unexpected height")
		}
		// Sunny-day
		return blocks[0], nil
	case 2:
		if height != "380641" {
			testT.Error("unexpected height")
		}
		// Sunny-day
		return blocks[1], nil
	case 3:
		if height != "380642" {
			testT.Error("unexpected height", height)
		}
		// Simulate that we're synced (caught up);
		// this should cause one 10s sleep (then retry).
		return nil, errors.New("-8: Block height out of range")
	case 4:
		if sleepCount != 1 || sleepDuration != 2*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		if height != "380642" {
			testT.Error("unexpected height", height)
		}
		// Simulate that we're still caught up; this should cause a 1s
		// wait then a check for reorg to shorter chain (back up one).
		return nil, errors.New("-8: Block height out of range")
	case 5:
		if sleepCount != 1 || sleepDuration != 2*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		// Back up to 41.
		if height != "380641" {
			testT.Error("unexpected height", height)
		}
		// Return the expected block (as normally happens, no actual reorg),
		// ingestor will immediately re-request the next block (42).
		return blocks[1], nil
	case 6:
		if sleepCount != 1 || sleepDuration != 2*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		if height != "380642" {
			testT.Error("unexpected height", height)
		}
		// Block 42 has now finally appeared, it will immediately ask for 43.
		return blocks[2], nil
	case 7:
		if sleepCount != 1 || sleepDuration != 2*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		if height != "380643" {
			testT.Error("unexpected height", height)
		}
		// Simulate a reorg by modifying the block's hash temporarily,
		// this causes a 1s sleep and then back up one block (to 42).
		blocks[3][9]++ // first byte of the prevhash
		return blocks[3], nil
	case 8:
		blocks[3][9]-- // repair first byte of the prevhash
		if sleepCount != 1 || sleepDuration != 2*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		if height != "380642" {
			testT.Error("unexpected height ", height)
		}
		return blocks[2], nil
	case 9:
		if sleepCount != 1 || sleepDuration != 2*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		if height != "380643" {
			testT.Error("unexpected height ", height)
		}
		// Instead of returning expected (43), simulate block unmarshal
		// failure, should cause 10s sleep, retry
		return nil, nil
	case 10:
		if sleepCount != 2 || sleepDuration != 12*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		if height != "380643" {
			testT.Error("unexpected height ", height)
		}
		// Back to sunny-day
		return blocks[3], nil
	case 11:
		if sleepCount != 2 || sleepDuration != 12*time.Second {
			testT.Error("unexpected sleeps", sleepCount, sleepDuration)
		}
		if height != "380644" {
			testT.Error("unexpected height ", height)
		}
		// next block not ready
		return nil, nil
	}
	testT.Error("getblockStub called too many times")
	return nil, nil
}

func TestGetBlockRange(t *testing.T) {
	testT = t
	RawRequest = getblockStub
	testcache = NewBlockCache(testCacheDB(t), unitTestChain, 380640, true)
	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error)
	go GetBlockRange(context.Background(), testcache, blockChan, errChan, 380640, 380642)

	// read in block 380640
	select {
	case err := <-errChan:
		// this will also catch context.DeadlineExceeded from the timeout
		t.Fatal("unexpected error:", err)
	case cBlock := <-blockChan:
		if cBlock.Height != 380640 {
			t.Fatal("unexpected Height:", cBlock.Height)
		}
	}

	// read in block 380641
	select {
	case err := <-errChan:
		// this will also catch context.DeadlineExceeded from the timeout
		t.Fatal("unexpected error:", err)
	case cBlock := <-blockChan:
		if cBlock.Height != 380641 {
			t.Fatal("unexpected Height:", cBlock.Height)
		}
	}

	// try to read in block 380642, but this will fail (see case 3 above)
	select {
	case err := <-errChan:
		// this will also catch context.DeadlineExceeded from the timeout
		if err.Error() != "block requested is newer than latest block" {
			t.Fatal("unexpected error:", err)
		}
	case _ = <-blockChan:
		t.Fatal("reading height 22 should have failed")
	}

	step = 0
}

// staleForkCache returns a cache holding a block with a mismatched hash to simulate a reorg.
func staleForkCache(t *testing.T) *BlockCache {
	cache := NewBlockCache(testCacheDB(t), unitTestChain, 380640, false)
	err := cache.Add(380640, &walletrpc.CompactBlock{
		Height:   380640,
		Hash:     bytes.Repeat([]byte{0xa1}, 32),
		PrevHash: bytes.Repeat([]byte{0xa0}, 32),
	})
	if err != nil {
		t.Fatal("cache.Add failed:", err)
	}
	return cache
}

// discontinuityStub mocks RPC getblock for height 380641.
func discontinuityStub(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
	if method != "getblock" {
		testT.Error("unexpected method")
	}
	var height string
	if err := json.Unmarshal(params[0], &height); err != nil {
		testT.Fatal("could not unmarshal height")
	}

	step++
	switch step {
	case 1:
		if height != "380641" {
			testT.Error("unexpected height", height)
		}
		return blocks[1], nil
	}
	testT.Error("discontinuityStub called too many times")
	return nil, nil
}

// resetGlobals restores global state between test runs.
func resetGlobals() {
	step = 0
	sleepCount = 0
	sleepDuration = 0
	RawRequest = nil
	Time.Sleep = nil
	Time.Now = nil
	Time.After = nil
	g_lastBlockChainInfo = &ZcashdRpcReplyGetblockchaininfo{}
	g_lastTime = time.Time{}
	g_txidSeen = map[txid]struct{}{}
	g_txList = []*walletrpc.RawTransaction{}
}

// checkDiscontinuity verifies GetBlockRange returns a discontinuity error.
func checkDiscontinuity(t *testing.T, blockChan <-chan *walletrpc.CompactBlock, errChan <-chan error) {
	t.Helper()
	select {
	case err := <-errChan:
		if status.Code(err) != codes.Aborted {
			t.Fatal("unexpected error code:", status.Code(err), err)
		}
		if !strings.Contains(err.Error(), "chain discontinuity") {
			t.Fatal("unexpected error:", err)
		}
	case cBlock := <-blockChan:
		t.Fatal("streamed a block that doesn't connect, height:", cBlock.Height)
	}
}

func TestGetBlockRangeDiscontinuity(t *testing.T) {
	testT = t
	RawRequest = discontinuityStub
	defer resetGlobals()
	testcache = staleForkCache(t)

	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error)
	go GetBlockRange(context.Background(), testcache, blockChan, errChan, 380640, 380641)

	// The stale 380640 goes out before there's anything to compare it
	// against; the mismatch can only be detected once 380641 arrives.
	select {
	case err := <-errChan:
		t.Fatal("unexpected error:", err)
	case cBlock := <-blockChan:
		if cBlock.Height != 380640 {
			t.Fatal("unexpected Height:", cBlock.Height)
		}
	}

	// blocks[1].PrevHash is the real 380640 hash, not the stale one.
	checkDiscontinuity(t, blockChan, errChan)

	if step != 1 {
		t.Fatal("unexpected step:", step)
	}
}

// Same as TestGetBlockRangeDiscontinuity, but with start greater than end, so
// the blocks are compared in the other direction.
func TestGetBlockRangeDiscontinuityReverse(t *testing.T) {
	testT = t
	RawRequest = discontinuityStub
	defer resetGlobals()
	testcache = staleForkCache(t)

	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error)
	go GetBlockRange(context.Background(), testcache, blockChan, errChan, 380641, 380640)

	// read in block 380641 (from the backend, the current fork)
	select {
	case err := <-errChan:
		t.Fatal("unexpected error:", err)
	case cBlock := <-blockChan:
		if cBlock.Height != 380641 {
			t.Fatal("unexpected Height:", cBlock.Height)
		}
	}

	// The stale cached 380640 isn't the block that 380641 descends from.
	checkDiscontinuity(t, blockChan, errChan)

	if step != 1 {
		t.Fatal("unexpected step:", step)
	}
}

// The same cache/backend split, but with a cached block that really is the
// parent of the backend's block: the range must stream normally. Without this,
// nothing covers the happy path across the cache/backend boundary, which is
// every wallet syncing at the cache tip.
func TestGetBlockRangeContiguousReverse(t *testing.T) {
	testT = t
	RawRequest = discontinuityStub
	defer resetGlobals()
	testcache = NewBlockCache(testCacheDB(t), unitTestChain, 380640, false)

	// Cache the real 380640, the block that the backend's 380641 descends from.
	block := parser.NewBlock()
	var blockHex string
	if err := json.Unmarshal(blocks[0], &blockHex); err != nil {
		t.Fatal("could not unmarshal test block:", err)
	}
	blockBytes, err := hex.DecodeString(blockHex)
	if err != nil {
		t.Fatal("could not decode test block:", err)
	}
	if _, err := block.ParseFromSlice(blockBytes); err != nil {
		t.Fatal("could not parse test block:", err)
	}
	if err := testcache.Add(380640, block.ToCompact()); err != nil {
		t.Fatal("cache.Add failed:", err)
	}

	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error)
	go GetBlockRange(context.Background(), testcache, blockChan, errChan, 380641, 380640)

	for _, height := range []uint64{380641, 380640} {
		select {
		case err := <-errChan:
			t.Fatal("unexpected error:", err)
		case cBlock := <-blockChan:
			if cBlock.Height != height {
				t.Fatal("unexpected Height:", cBlock.Height)
			}
		}
	}
	if err := <-errChan; err != nil {
		t.Fatal("unexpected error:", err)
	}
}

func TestGetBlockRangeCancelsInFlightRPC(t *testing.T) {
	RawRequest = func(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	testcache = NewBlockCache(testCacheDB(t), unitTestChain, 380640, false)
	ctx, cancel := context.WithCancel(context.Background())
	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		GetBlockRange(ctx, testcache, blockChan, errChan, 380640, 380640)
		close(done)
	}()
	cancel()
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("GetBlockRange did not exit after context cancellation")
	case <-done:
	}
}

// There are four test blocks, 0..3
func getblockStubReverse(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
	var height string
	err := json.Unmarshal(params[0], &height)
	if err != nil {
		testT.Fatal("could not unmarshal height")
	}

	step++
	switch step {
	case 1:
		if height != "380642" {
			testT.Error("unexpected height")
		}
		// Sunny-day
		return blocks[2], nil
	case 2:
		if height != "380641" {
			testT.Error("unexpected height")
		}
		// Sunny-day
		return blocks[1], nil
	case 3:
		if height != "380640" {
			testT.Error("unexpected height")
		}
		// Sunny-day
		return blocks[0], nil
	}
	testT.Error("getblockStub called too many times")
	return nil, nil
}

func TestGetBlockRangeReverse(t *testing.T) {
	testT = t
	RawRequest = getblockStubReverse
	testcache = NewBlockCache(testCacheDB(t), unitTestChain, 380640, true)
	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error)

	// Request the blocks in reverse order by specifying start greater than end
	go GetBlockRange(context.Background(), testcache, blockChan, errChan, 380642, 380640)

	// read in block 380642
	select {
	case err := <-errChan:
		// this will also catch context.DeadlineExceeded from the timeout
		t.Fatal("unexpected error:", err)
	case cBlock := <-blockChan:
		if cBlock.Height != 380642 {
			t.Fatal("unexpected Height:", cBlock.Height)
		}
	}

	// read in block 380641
	select {
	case err := <-errChan:
		// this will also catch context.DeadlineExceeded from the timeout
		t.Fatal("unexpected error:", err)
	case cBlock := <-blockChan:
		if cBlock.Height != 380641 {
			t.Fatal("unexpected Height:", cBlock.Height)
		}
	}

	// read in block 380640
	select {
	case err := <-errChan:
		// this will also catch context.DeadlineExceeded from the timeout
		t.Fatal("unexpected error:", err)
	case cBlock := <-blockChan:
		if cBlock.Height != 380640 {
			t.Fatal("unexpected Height:", cBlock.Height)
		}
	}
	step = 0
}

func TestGenerateCerts(t *testing.T) {
	if GenerateCerts() == nil {
		t.Fatal("GenerateCerts returned nil")
	}
}

// ------------------------------------------ GetMempoolStream

// Note that in mocking zcashd's RPC replies here, we don't really need
// actual txids or transactions, or even strings with the correct format
// for those, except that a transaction must be a hex string.
func mempoolStub(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
	step++
	switch step {
	case 1:
		// This will be a getblockchaininfo request
		if method != "getblockchaininfo" {
			testT.Fatal("expecting blockchaininfo")
		}
		r, _ := json.Marshal(&ZcashdRpcReplyGetblockchaininfo{
			BestBlockHash: "010203",
			Blocks:        200,
		})
		return r, nil
	case 2:
		// No new block has arrived.
		if method != "getblockchaininfo" {
			testT.Fatal("expecting blockchaininfo")
		}
		r, _ := json.Marshal(&ZcashdRpcReplyGetblockchaininfo{
			BestBlockHash: "010203",
			Blocks:        200,
		})
		return r, nil
	case 3:
		// Expect a getrawmempool next.
		if method != "getrawmempool" {
			testT.Fatal("expecting getrawmempool")
		}
		// In reality, this would be a hex txid
		r, _ := json.Marshal([]string{
			"mempooltxid-1",
		})
		return r, nil
	case 4:
		// Next, it should ask for this transaction (non-verbose).
		if method != "getrawtransaction" {
			testT.Fatal("expecting getrawtransaction")
		}
		var txid string
		json.Unmarshal(params[0], &txid)
		if txid != "mempooltxid-1" {
			testT.Fatal("unexpected txid")
		}
		r, _ := json.Marshal(map[string]string{"hex": "aabb"})
		return r, nil
	case 5:
		// Simulate that still no new block has arrived ...
		if method != "getblockchaininfo" {
			testT.Fatal("expecting blockchaininfo")
		}
		r, _ := json.Marshal(&ZcashdRpcReplyGetblockchaininfo{
			BestBlockHash: "010203",
			Blocks:        200,
		})
		return r, nil
	case 6:
		// ... but there a second tx has arrived in the mempool
		if method != "getrawmempool" {
			testT.Fatal("expecting getrawmempool")
		}
		// In reality, this would be a hex txid
		r, _ := json.Marshal([]string{
			"mempooltxid-2",
			"mempooltxid-1"})
		return r, nil
	case 7:
		// The new mempool tx (and only that one) gets fetched
		if method != "getrawtransaction" {
			testT.Fatal("expecting getrawtransaction")
		}
		var txid string
		json.Unmarshal(params[0], &txid)
		if txid != "mempooltxid-2" {
			testT.Fatal("unexpected txid")
		}
		r, _ := json.Marshal(map[string]string{"hex": "ccdd"})
		return r, nil
	case 8:
		// A new block arrives, this will cause these two tx to be returned
		if method != "getblockchaininfo" {
			testT.Fatal("expecting blockchaininfo")
		}
		r, _ := json.Marshal(&ZcashdRpcReplyGetblockchaininfo{
			BestBlockHash: "d1d2d3",
			Blocks:        201,
		})
		return r, nil
	}
	testT.Fatal("ran out of cases")
	return nil, nil
}

func TestMempoolStream(t *testing.T) {
	testT = t
	RawRequest = mempoolStub
	Time.Sleep = sleepStub
	Time.Now = nowStub
	Time.After = afterStub
	// In real life, wall time is not close to zero, simulate that.
	sleepDuration = 1000 * time.Second

	var replies []*walletrpc.RawTransaction
	// The first request after startup immediately returns an empty list.
	err := GetMempool(context.Background(), func(tx *walletrpc.RawTransaction) error {
		t.Fatal("send to client function called on initial GetMempool call")
		return nil
	})
	if err != nil {
		t.Errorf("GetMempool failed: %v", err)
	}

	// This should return two transactions.
	err = GetMempool(context.Background(), func(tx *walletrpc.RawTransaction) error {
		replies = append(replies, tx)
		return nil
	})
	if err != nil {
		t.Errorf("GetMempool failed: %v", err)
	}
	if len(replies) != 2 {
		t.Fatal("unexpected number of tx")
	}
	// The interface guarantees that the transactions will be returned
	// in the order they entered the mempool.
	if !bytes.Equal([]byte(replies[0].GetData()), []byte{0xaa, 0xbb}) {
		t.Fatal("unexpected tx contents")
	}
	if replies[0].GetHeight() != 0 {
		t.Fatal("unexpected tx height")
	}
	if !bytes.Equal([]byte(replies[1].GetData()), []byte{0xcc, 0xdd}) {
		t.Fatal("unexpected tx contents")
	}
	if replies[1].GetHeight() != 0 {
		t.Fatal("unexpected tx height")
	}

	// Time started at 1000 seconds (since 1970), and just over 4 seconds
	// should have elapsed. The units here are nanoseconds.
	if sleepDuration != 1004400000000 {
		t.Fatal("unexpected end time")
	}
	if step != 8 {
		t.Fatal("unexpected number of zcashd RPCs")
	}

	step = 0
	sleepCount = 0
	sleepDuration = 0
}

func TestParseRawTransaction(t *testing.T) {
	rt0, err0 := ParseRawTransaction([]byte("{\"hex\": \"deadbeef\", \"height\": 123456}"))
	if err0 != nil {
		t.Fatal("Failed to parse raw transaction response with known height.")
	}
	if rt0.Height != 123456 {
		t.Errorf("Unmarshalled incorrect height: got: %d, expected: 123456.", rt0.Height)
	}

	rt1, err1 := ParseRawTransaction([]byte("{\"hex\": \"deadbeef\", \"height\": -1}"))
	if err1 != nil {
		t.Fatal("Failed to parse raw transaction response for a known tx not in the main chain.")
	}
	// We expect the int64 value `-1` to have been reinterpreted as a uint64 value in order
	// to be representable as a uint64 in `RawTransaction`. The conversion from the twos-complement
	// signed representation should map `-1` to `math.MaxUint64`.
	if rt1.Height != math.MaxUint64 {
		t.Errorf("Unmarshalled incorrect height: got: %d, want: 0x%X.", rt1.Height, uint64(math.MaxUint64))
	}

	rt2, err2 := ParseRawTransaction([]byte("{\"hex\": \"deadbeef\"}"))
	if err2 != nil {
		t.Fatal("Failed to parse raw transaction response for a tx in the mempool.")
	}
	if rt2.Height != 0 {
		t.Errorf("Unmarshalled incorrect height: got: %d, expected: 0.", rt2.Height)
	}
}

// TestMempoolStreamCancelOnEmptyMempool is the regression test for the fix in
// this PR. Without the fix, GetMempool on an empty mempool with stable tip
// hash never observes ctx.Done because sendToClient is never invoked and the
// 200ms Time.Sleep is non-cancellable. With the fix, the cancel-aware select
// at the bottom of the loop returns promptly with ctx.Err().
func TestMempoolStreamCancelOnEmptyMempool(t *testing.T) {
	// Stub RawRequest to return a stable empty-mempool / stable-tip world,
	// so the loop parks at the cancel-aware sleep with no work and no tip
	// change to break out.
	RawRequest = func(ctx context.Context, method string, params []json.RawMessage) (json.RawMessage, error) {
		switch method {
		case "getblockchaininfo":
			r, _ := json.Marshal(&ZcashdRpcReplyGetblockchaininfo{
				BestBlockHash: "stable-hash",
				Blocks:        100,
			})
			return r, nil
		case "getrawmempool":
			return json.RawMessage("[]"), nil
		}
		return nil, errors.New("unexpected RPC: " + method)
	}
	// Real time for the cancel-aware sleep so ctx.Done has a real race with
	// the 200ms timer. afterStub would fire instantly and the test would not
	// exercise the cancel path deterministically.
	Time.After = time.After
	Time.Sleep = sleepStub
	Time.Now = time.Now

	// Pre-populate the package-global tip cache so the first refresh matches
	// the stubbed tip and does NOT trigger the tip-changed branch (which
	// would break out of the loop immediately).
	g_lastBlockChainInfo = &ZcashdRpcReplyGetblockchaininfo{BestBlockHash: "stable-hash"}
	g_lastTime = time.Time{}
	g_txidSeen = map[txid]struct{}{}
	g_txList = []*walletrpc.RawTransaction{}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- GetMempool(ctx, func(tx *walletrpc.RawTransaction) error {
			t.Error("sendToClient must not be invoked on empty mempool")
			return nil
		})
	}()

	// Let the loop reach the cancel-aware select.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetMempool did not return within 2s of cancel; the cancel-aware select is not effective")
	}

	// Reset shared state for any subsequent tests.
	g_lastBlockChainInfo = &ZcashdRpcReplyGetblockchaininfo{}
	g_lastTime = time.Time{}
	g_txidSeen = map[txid]struct{}{}
	g_txList = []*walletrpc.RawTransaction{}
	sleepCount = 0
	sleepDuration = 0
}
