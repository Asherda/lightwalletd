// Copyright (c) 2026 The Verus developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .
package parser

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/veruscoin/lightwalletd/parser/verushash"
)

type solutionVersionTest struct {
	BlockHeight     int    `json:"block"`
	SolutionVersion int    `json:"solutionVersion"`
	BlockHash       string `json:"hash"`
	PrevHash        string `json:"prev"`
	Full            string `json:"full"`
}

func TestSolutionVersionHashes(t *testing.T) {
	blockJSON, err := ioutil.ReadFile("../testdata/solution_version_blocks.json")
	if err != nil {
		t.Fatal(err)
	}
	var tests []solutionVersionTest
	if err := json.Unmarshal(blockJSON, &tests); err != nil {
		t.Fatal(err)
	}
	if len(tests) == 0 {
		t.Fatal("no solution version test blocks")
	}

	seen := make(map[int]bool)
	for _, test := range tests {
		blockData, err := hex.DecodeString(test.Full)
		if err != nil {
			t.Errorf("block %d: %v", test.BlockHeight, err)
			continue
		}

		block := NewBlock()
		rest, err := block.ParseFromSlice(blockData)
		if err != nil {
			t.Errorf("block %d: %v", test.BlockHeight, err)
			continue
		}
		if len(rest) > 0 {
			t.Errorf("block %d: %d bytes remaining", test.BlockHeight, len(rest))
			continue
		}

		solutionVersion := int(block.hdr.Solution[0])
		if solutionVersion != test.SolutionVersion {
			t.Errorf("block %d: solution version is %d, want %d",
				test.BlockHeight, solutionVersion, test.SolutionVersion)
			continue
		}
		seen[solutionVersion] = true

		if block.GetHeight() != test.BlockHeight {
			t.Errorf("block %d: parsed height %d", test.BlockHeight, block.GetHeight())
			continue
		}
		if got := hex.EncodeToString(block.GetDisplayHash()); got != test.BlockHash {
			t.Errorf("block %d (solution version %d): wrong hash\nhave: %s\nwant: %s",
				test.BlockHeight, solutionVersion, got, test.BlockHash)
		}
		if got := hex.EncodeToString(block.GetDisplayPrevHash()); got != test.PrevHash {
			t.Errorf("block %d (solution version %d): wrong prevhash\nhave: %s\nwant: %s",
				test.BlockHeight, solutionVersion, got, test.PrevHash)
		}

		serialized, err := block.hdr.MarshalBinary()
		if err != nil {
			t.Errorf("block %d: serializing header: %v", test.BlockHeight, err)
			continue
		}
		canonical := hex.EncodeToString(Reverse(verushash.HashHeader(serialized)))
		if canonical != test.BlockHash {
			t.Errorf("block %d (solution version %d): HashHeader disagrees with chain\nhave: %s\nwant: %s",
				test.BlockHeight, solutionVersion, canonical, test.BlockHash)
		}
	}

	for _, want := range []int{0, 1, 3, 5, 6, 7, 8} {
		if !seen[want] {
			t.Errorf("no test block for solution version %d", want)
		}
	}
}

func TestGenesisBlockHash(t *testing.T) {
	const genesisHash = "027e3758c3a65b12aa1046462b486d0a63bfa1beae327897f56c5cfb7daaae71"

	blockFile, err := ioutil.ReadFile("../testdata/mainnet_genesis")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitN(string(blockFile), "\n", 2)
	blockData, err := hex.DecodeString(strings.TrimSpace(lines[0]))
	if err != nil {
		t.Fatal(err)
	}

	block := NewBlock()
	rest, err := block.ParseFromSlice(blockData)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) > 0 {
		t.Errorf("%d bytes remaining", len(rest))
	}
	if got := hex.EncodeToString(block.GetDisplayHash()); got != genesisHash {
		t.Errorf("wrong genesis hash\nhave: %s\nwant: %s", got, genesisHash)
	}

	serialized, err := block.hdr.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(Reverse(verushash.VerusHash(serialized))); got == genesisHash {
		t.Error("raw VerusHash now matches genesis; this test can no longer detect the regression")
	}
}

// TestHashHeaderRejectsMalformed verifies that invalid headers return nil.
func TestHashHeaderRejectsMalformed(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"single byte", []byte{4}},
		{"truncated", make([]byte, 10)},
		{"one short of a bare header", make([]byte, serBlockHeaderMinusEquihashSize-1)},
	}
	for _, test := range tests {
		if got := verushash.HashHeader(test.input); got != nil {
			t.Errorf("%s: got %x, want nil", test.name, got)
		}
	}
}
