// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .
package parser

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/pkg/errors"

	protobuf "github.com/golang/protobuf/proto"
)

func TestBlockParser(t *testing.T) {
	var txhashes = [][]string{
		{
			"d3badb19e0a067d800c2d6fec1024e57e04fb5c9e5b65fd1e84f42405c784167",
			"858437d350bf2e943fb3cc3a678c858769c8cba6ff538f86ccfc944e497a4672",
		}, {
			"ed047dd498f48a1ee15362861824961741d9c4e847f66c84effacb09249f5d6d",
			"2259a6d5c446910d674791b20facd246f98f7757160f98fd8d90df5d182a9455",
		}, {
			"1d142b63352c8fbd6df57f06b5e21df5857955d571ca911ada9ecc9e16f7bb6f",
			"513a7915b5a065253f3900fd0dab23e4670fb96e7f3d25ba45cbc82ec8e11434",
			"441eb6a6e78fe8a6ba3132af8e7662f7970d07ef8511fd4c61be6421429cf68c",
		}, {
			"7c61ac160b0fd9a7c6c4fffd2c261c9fd9a34468a4b3d943068be98247aa730f",
			"8f7233fba30716e0207a47c5ed00279d5b2004e4e3f0803679ff8c2b0f206f07",
			"7b00e0751480f71d65e28b1e092c2367d0fea9056163ad8e580ef08f5c8d04b0",
		},
	}
	var hasSapling = []bool{false, false, true, true}
	testBlocks, err := os.Open("../testdata/blocks")
	if err != nil {
		t.Fatal(err)
	}
	defer testBlocks.Close()

	scan := bufio.NewScanner(testBlocks)
	for blockindex := 0; scan.Scan(); blockindex++ {
		blockDataHex := scan.Text()
		blockData, err := hex.DecodeString(blockDataHex)
		if err != nil {
			t.Error(err)
			continue
		}

		// This is just a sanity check of the test:
		if int(blockData[1487]) != len(txhashes[blockindex]) {
			t.Error("wrong number of transactions, test broken?")
		}

		// Make a copy of just the transactions alone, which,
		// for these blocks, start just beyond the header and
		// the one-byte nTx value, which is offset 1488.
		transactions := make([]byte, len(blockData[1488:]))
		copy(transactions, blockData[1488:])

		// Each iteration of this loop appends the block's original
		// transactions, so we build an ever-larger block. The loop
		// limit is arbitrary, but make sure we get into double-digit
		// transaction counts (compact integer).
		for i := 0; i < 264; i++ {
			b := blockData
			block := NewBlock()
			b, err = block.ParseFromSlice(b)
			if err != nil {
				t.Error(errors.Wrap(err, fmt.Sprintf("parsing block %d", i)))
				continue
			}
			if len(b) > 0 {
				t.Error("Extra data remaining")
			}

			// Some basic sanity checks
			if block.hdr.Version != posNonceVerusV2 {
				t.Error("Read wrong version in a test block.")
				break
			}
			if block.GetVersion() != posNonceVerusV2 {
				t.Error("Read wrong version in a test block.")
				break
			}
			if block.GetTxCount() < 1 {
				t.Error("No transactions in block")
				break
			}
			if len(block.Transactions()) != block.GetTxCount() {
				t.Error("Number of transactions mismatch")
				break
			}
			if block.GetTxCount() != len(txhashes[blockindex])*(i+1) {
				t.Error("Unexpected number of transactions")
			}
			if block.HasSaplingTransactions() != hasSapling[blockindex] {
				t.Errorf("block %d: HasSaplingTransactions is %v, want %v",
					blockindex, block.HasSaplingTransactions(), hasSapling[blockindex])
				break
			}
			anySapling := false
			for txindex, tx := range block.Transactions() {
				if tx.HasSaplingElements() {
					anySapling = true
				}
				expectedHash := txhashes[blockindex][txindex%len(txhashes[blockindex])]
				if hex.EncodeToString(tx.GetDisplayHash()) != expectedHash {
					t.Error("incorrect tx hash")
				}
			}
			if anySapling != block.HasSaplingTransactions() {
				t.Errorf("block %d: HasSaplingTransactions is %v but per-tx scan found %v",
					blockindex, block.HasSaplingTransactions(), anySapling)
				break
			}
			// Keep appending the original transactions, which is unrealistic
			// because the coinbase is being replicated, but it works; first do
			// some surgery to the transaction count (see DarksideApplyStaged()).
			for j := 0; j < len(txhashes[blockindex]); j++ {
				nTxFirstByte := blockData[1487]
				switch {
				case nTxFirstByte < 252:
					blockData[1487]++
				case nTxFirstByte == 252:
					// incrementing to 253, requires "253" followed by 2-byte length,
					// extend the block by two bytes, shift existing transaction bytes
					blockData = append(blockData, 0, 0)
					copy(blockData[1490:], blockData[1488:len(blockData)-2])
					blockData[1487] = 253
					blockData[1488] = 253
					blockData[1489] = 0
				case nTxFirstByte == 253:
					blockData[1488]++
					if blockData[1488] == 0 {
						// wrapped around
						blockData[1489]++
					}
				}
			}
			blockData = append(blockData, transactions...)
		}
	}
}

func TestBlockParserFail(t *testing.T) {
	testBlocks, err := os.Open("../testdata/badblocks")
	if err != nil {
		t.Fatal(err)
	}
	defer testBlocks.Close()

	scan := bufio.NewScanner(testBlocks)

	// the first "block" contains an illegal hex character
	{
		scan.Scan()
		blockDataHex := scan.Text()
		_, err := hex.DecodeString(blockDataHex)
		if err == nil {
			t.Error("unexpected success parsing illegal hex bad block")
		}
	}
	for i := 0; scan.Scan(); i++ {
		blockDataHex := scan.Text()
		blockData, err := hex.DecodeString(blockDataHex)
		if err != nil {
			t.Error(err)
			continue
		}

		block := NewBlock()
		blockData, err = block.ParseFromSlice(blockData)
		if err == nil {
			t.Error("unexpected success parsing bad block")
		}
	}
}

// Checks on the first 20 blocks from mainnet genesis.
func TestGenesisBlockParser(t *testing.T) {
	blockFile, err := os.Open("../testdata/mainnet_genesis")
	if err != nil {
		t.Fatal(err)
	}
	defer blockFile.Close()

	scan := bufio.NewScanner(blockFile)
	for i := 0; scan.Scan(); i++ {
		blockDataHex := scan.Text()
		blockData, err := hex.DecodeString(blockDataHex)
		if err != nil {
			t.Error(err)
			continue
		}

		block := NewBlock()
		blockData, err = block.ParseFromSlice(blockData)
		if err != nil {
			t.Error(err)
			continue
		}
		if len(blockData) > 0 {
			t.Error("Extra data remaining")
		}

		// Genesis block has version 1 and no BIP34 height.
		if i == 0 {
			if block.hdr.Version != 1 {
				t.Errorf("Read wrong version in genesis block: %d", block.hdr.Version)
				break
			}
			continue
		}

		// Some basic sanity checks
		if block.hdr.Version != 4 {
			t.Error("Read wrong version in genesis block.")
			break
		}

		if block.GetHeight() != i {
			t.Errorf("Got wrong height for block %d: %d", i, block.GetHeight())
		}
	}
}

func TestCompactBlocks(t *testing.T) {
	type compactTest struct {
		BlockHeight int    `json:"block"`
		BlockHash   string `json:"hash"`
		PrevHash    string `json:"prev"`
		Full        string `json:"full"`
		Compact     string `json:"compact"`
	}
	var compactTests []compactTest

	blockJSON, err := ioutil.ReadFile("../testdata/compact_blocks.json")
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(blockJSON, &compactTests)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range compactTests {
		blockData, _ := hex.DecodeString(test.Full)
		block := NewBlock()
		blockData, err = block.ParseFromSlice(blockData)
		if err != nil {
			t.Error(errors.Wrap(err, fmt.Sprintf("parsing testnet block %d", test.BlockHeight)))
			continue
		}
		if len(blockData) > 0 {
			t.Error("Extra data remaining")
		}
		if block.GetHeight() != test.BlockHeight {
			t.Errorf("incorrect block height in testnet block %d", test.BlockHeight)
			continue
		}
		if hex.EncodeToString(block.GetDisplayHash()) != test.BlockHash {
			t.Errorf("incorrect block hash in testnet block %x", test.BlockHash)
			continue
		}
		if hex.EncodeToString(block.GetDisplayPrevHash()) != test.PrevHash {
			t.Errorf("incorrect block prevhash in testnet block %x", test.BlockHash)
			continue
		}
		if !bytes.Equal(block.GetPrevHash(), block.hdr.HashPrevBlock) {
			t.Error("block and block header prevhash don't match")
		}

		compact := block.ToCompact()
		marshaled, err := protobuf.Marshal(compact)
		if err != nil {
			t.Errorf("could not marshal compact testnet block %d", test.BlockHeight)
			continue
		}
		encodedCompact := hex.EncodeToString(marshaled)
		if encodedCompact != test.Compact {
			t.Errorf("wrong data for compact testnet block %d\nhave: %s\nwant: %s\n", test.BlockHeight, encodedCompact, test.Compact)
			break
		}
	}

}

func TestParseBlockRejectsTransactionCountThatCannotFit(t *testing.T) {
	// 141-byte block header with version=4, zero-valued header fields, and an
	// empty CompactSize-prefixed Equihash solution, followed by tx_count=1 and
	// no transaction bytes.
	blockData := make([]byte, 141)
	blockData[0] = 0x04
	blockData = append(blockData, 0x01)

	block := NewBlock()
	_, err := block.ParseFromSlice(blockData)
	if err == nil {
		t.Fatal("expected error")
	}
	wantErr := "tx_count 1 requires at least 10 bytes, but only 0 remain"
	if !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("error mismatch:\nhave: %v\nwant substring: %s", err, wantErr)
	}
}
