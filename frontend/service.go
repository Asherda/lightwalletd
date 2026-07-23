// Copyright (c) 2019-2020 The Zcash developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or https://www.opensource.org/licenses/mit-license.php .

// Package frontend implements the gRPC handlers called by the wallets.
package frontend

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/asherda/lightwalletd/common"
	"github.com/asherda/lightwalletd/parser"
	"github.com/asherda/lightwalletd/walletrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lwdStreamer struct {
	cache      *common.BlockCache
	chainName  string
	pingEnable bool
	walletrpc.UnimplementedCompactTxStreamerServer
}

// NewLwdStreamer constructs a gRPC context.
func NewLwdStreamer(cache *common.BlockCache, chainName string, enablePing bool) (walletrpc.CompactTxStreamerServer, error) {
	return &lwdStreamer{cache: cache, chainName: chainName, pingEnable: enablePing}, nil
}

// DarksideStreamer holds the gRPC state for darksidewalletd.
type DarksideStreamer struct {
	cache *common.BlockCache
	walletrpc.UnimplementedDarksideStreamerServer
}

// NewDarksideStreamer constructs a gRPC context for darksidewalletd.
func NewDarksideStreamer(cache *common.BlockCache) (walletrpc.DarksideStreamerServer, error) {
	return &DarksideStreamer{cache: cache}, nil
}

// Test to make sure Address is a single t address
func checkTaddress(taddr string) error {
	match, err := regexp.Match("\\AR[a-zA-Z0-9]{33}\\z", []byte(taddr))
	if err != nil {
		return status.Errorf(codes.InvalidArgument,
			"checkTaddress: invalid transparent address: %s error: %s", taddr, err.Error())
	}
	if !match {
		return status.Errorf(codes.InvalidArgument,
			"checkTaddress: transparent address %s contains invalid characters", taddr)
	}
	return nil
}

// GetLatestBlock returns the height of the best chain, according to zcashd.
func (s *lwdStreamer) GetLatestBlock(ctx context.Context, placeholder *walletrpc.ChainSpec) (*walletrpc.BlockID, error) {
	common.Log.Debugf("gRPC GetLatestBlock(%+v)\n", placeholder)
	latestBlock := s.cache.GetLatestHeight()
	latestHash := s.cache.GetLatestHash()

	if latestBlock == -1 {
		return nil, errors.New("Cache is empty. Server is probably not yet ready")
	}

	r := &walletrpc.BlockID{Height: uint64(latestBlock), Hash: latestHash}
	common.Log.Tracef("  return: %+v\n", r)
	return r, nil
}

// GetTaddressTxids is a streaming RPC that returns transaction IDs that have
// the given transparent address (taddr) as either an input or output.
func (s *lwdStreamer) GetTaddressTxids(addressBlockFilter *walletrpc.TransparentAddressBlockFilter, resp walletrpc.CompactTxStreamer_GetTaddressTxidsServer) error {
	common.Log.Debugf("gRPC GetTaddressTxids(%+v)\n", addressBlockFilter)
	if err := checkTaddress(addressBlockFilter.Address); err != nil {
		return err
	}

	if addressBlockFilter.Range == nil {
		return errors.New("Must specify block range")
	}
	if addressBlockFilter.Range.Start == nil {
		return errors.New("Must specify a start block height")
	}
	if addressBlockFilter.Range.End == nil {
		return errors.New("Must specify an end block height")
	}
	params := make([]json.RawMessage, 1)
	request := &common.ZcashdRpcRequestGetaddresstxids{
		Addresses: []string{addressBlockFilter.Address},
		Start:     addressBlockFilter.Range.Start.Height,
		End:       addressBlockFilter.Range.End.Height,
	}
	param, err := json.Marshal(request)
	if err != nil {
		return err
	}
	params[0] = param
	result, rpcErr := common.RawRequest(resp.Context(), "getaddresstxids", params)

	// For some reason, the error responses are not JSON
	if rpcErr != nil {
		return rpcErr
	}

	var txids []string
	err = json.Unmarshal(result, &txids)
	if err != nil {
		return err
	}

	timeout, cancel := context.WithTimeout(resp.Context(), 30*time.Second)
	defer cancel()

	for _, txidstr := range txids {
		txid, _ := hex.DecodeString(txidstr)
		// Txid is read as a string, which is in big-endian order. But when converting
		// to bytes, it should be little-endian
		tx, err := s.GetTransaction(timeout, &walletrpc.TxFilter{Hash: parser.Reverse(txid)})
		if err != nil {
			return err
		}
		if err = resp.Send(tx); err != nil {
			return err
		}
	}
	return nil
}

// GetBlock returns the compact block at the requested height. Requesting a
// block by hash is not yet supported.
func (s *lwdStreamer) GetBlock(ctx context.Context, id *walletrpc.BlockID) (*walletrpc.CompactBlock, error) {
	common.Log.Debugf("gRPC GetBlock(%+v)\n", id)
	if id.Height == 0 && id.Hash == nil {
		return nil, errors.New("request for unspecified identifier")
	}

	// Precedence: a hash is more specific than a height. If we have it, use it first.
	if id.Hash != nil {
		// TODO: Get block by hash
		return nil, errors.New("GetBlock by Hash is not yet implemented")
	}
	cBlock, err := common.GetBlock(ctx, s.cache, int(id.Height))

	if err != nil {
		return nil, err
	}

	common.Log.Tracef("  return: %+v\n", cBlock)
	return cBlock, err
}

// GetBlockRange is a streaming RPC that returns blocks, in compact form,
// (as also returned by GetBlock) from the block height 'start' to height
// 'end' inclusively.
func (s *lwdStreamer) GetBlockRange(span *walletrpc.BlockRange, resp walletrpc.CompactTxStreamer_GetBlockRangeServer) error {
	common.Log.Debugf("gRPC GetBlockRange(%+v)\n", span)
	if span.Start == nil || span.End == nil {
		return errors.New("Must specify start and end heights")
	}
	ctx := resp.Context()
	blockChan := make(chan *walletrpc.CompactBlock)
	errChan := make(chan error)
	go common.GetBlockRange(ctx, s.cache, blockChan, errChan, int(span.Start.Height), int(span.End.Height))

	for {
		select {
		case <-ctx.Done():
			// Client cancelled / deadline exceeded; the producer's select-on-ctx
			// will unblock its in-flight send and exit.
			return ctx.Err()
		case err := <-errChan:
			// this will also catch context.DeadlineExceeded from the timeout
			return err
		case cBlock := <-blockChan:
			err := resp.Send(cBlock)
			if err != nil {
				return err
			}
		}
	}
}

// GetTreeState returns the note commitment tree state corresponding to the given block.
// See section 3.7 of the Zcash protocol specification. It returns several other useful
// values also (even though they can be obtained using GetBlock).
// blockHashLen is the length in bytes of a Zcash block hash.
const blockHashLen = 32

// The block can be specified by either height or hash.
func (s *lwdStreamer) GetTreeState(ctx context.Context, id *walletrpc.BlockID) (*walletrpc.TreeState, error) {
	if id.Height == 0 && id.Hash == nil {
		return nil, errors.New("request for unspecified identifier")
	}
	// The Zcash z_gettreestate rpc accepts either a block height or block hash
	params := make([]json.RawMessage, 1)
	var hashJSON []byte
	if id.Height > 0 {
		heightJSON, err := json.Marshal(strconv.Itoa(int(id.Height)))
		if err != nil {
			return nil, err
		}
		common.Log.Debugf("gRPC GetTreeState(height=%+v)\n", id.Height)
		params[0] = heightJSON
	} else {
		// Reject a wrong-length hash before expanding it: the bytes below are
		// hex-encoded (doubling them) and JSON-marshalled before zcashd ever
		// sees them, so without this an unauthenticated client can force large
		// allocations here and parsing work in the backend with input that can
		// only ever be rejected (GHSA-q2c2-hpp9-58hm).
		if len(id.Hash) != blockHashLen {
			return nil, status.Errorf(codes.InvalidArgument,
				"GetTreeState: block hash has invalid length: %d", len(id.Hash))
		}
		// id.Hash is big-endian, keep in big-endian for the rpc
		hash := hex.EncodeToString(id.Hash)
		common.Log.Debugf("gRPC GetTreeState(hash=%+v)\n", hash)
		hashJSON, err := json.Marshal(hash)
		if err != nil {
			return nil, err
		}
		params[0] = hashJSON
	}
	var gettreestateReply common.ZcashdRpcReplyGettreestate
	for {
		// Hygiene companion to PR #560: observe client cancel between
		// RawRequest calls. In practice this loop terminates in one iteration
		// on the active chain because zcashd's z_gettreestate hard-stops the
		// SkipHash walk at the Sapling activation height (zcashd
		// src/rpc/blockchain.cpp:1411). This check is for symmetry with the
		// other streaming-RPC ctx-checks, not for DoS defense.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, rpcErr := common.RawRequest(ctx, "z_gettreestate", params)
		if rpcErr != nil {
			return nil, rpcErr
		}
		err := json.Unmarshal(result, &gettreestateReply)
		if err != nil {
			return nil, err
		}
		if gettreestateReply.Sapling.Commitments.FinalState != "" {
			break
		}
		if gettreestateReply.Sapling.SkipHash == "" {
			break
		}
		hashJSON, err = json.Marshal(gettreestateReply.Sapling.SkipHash)
		if err != nil {
			return nil, err
		}
		params[0] = hashJSON
	}
	if gettreestateReply.Sapling.Commitments.FinalState == "" {
		return nil, errors.New("zcashd did not return treestate")
	}
	r := &walletrpc.TreeState{
		Network: s.chainName,
		Height:  uint64(gettreestateReply.Height),
		Hash:    gettreestateReply.Hash,
		Time:    gettreestateReply.Time,
		Tree:    gettreestateReply.Sapling.Commitments.FinalState,
	}
	common.Log.Tracef("  return: %+v\n", r)
	return r, nil
}

func (s *lwdStreamer) GetLatestTreeState(ctx context.Context, in *walletrpc.Empty) (*walletrpc.TreeState, error) {
	common.Log.Debugf("gRPC GetLatestTreeState()\n")
	latestHeight := s.cache.GetLatestHeight()

	if latestHeight == -1 {
		return nil, errors.New("Cache is empty. Server is probably not yet ready")
	}
	r, err := s.GetTreeState(ctx, &walletrpc.BlockID{Height: uint64(latestHeight)})
	if err == nil {
		common.Log.Tracef("  return: %+v\n", r)
	}
	return r, err
}

// GetTransaction returns the raw transaction bytes that are returned
// by the zcashd 'getrawtransaction' RPC.
func (s *lwdStreamer) GetTransaction(ctx context.Context, txf *walletrpc.TxFilter) (*walletrpc.RawTransaction, error) {
	common.Log.Debugf("gRPC GetTransaction(%+v)\n", txf)
	if txf.Hash != nil {
		if len(txf.Hash) != 32 {
			return nil, errors.New("Transaction ID has invalid length")
		}
		txidJSON, err := json.Marshal(hex.EncodeToString(parser.Reverse(txf.Hash)))
		if err != nil {
			return nil, err
		}

		params := []json.RawMessage{txidJSON, json.RawMessage("1")}
		result, rpcErr := common.RawRequest(ctx, "getrawtransaction", params)
		if rpcErr != nil {
			// For some reason, the error responses are not JSON
			return nil, rpcErr
		}

		r, err := common.ParseRawTransaction(result)
		if err != nil {
			return nil, err
		}
		common.Log.Tracef("  return: %+v\n", r)
		return r, nil
	}

	if txf.Block != nil && txf.Block.Hash != nil {
		return nil, errors.New("Can't GetTransaction with a blockhash+num. Please call GetTransaction with txid")
	}
	return nil, errors.New("Please call GetTransaction with txid")
}

// GetLightdInfo gets the LightWalletD (this server) info, and includes information
// it gets from its backend zcashd.
func (s *lwdStreamer) GetLightdInfo(ctx context.Context, in *walletrpc.Empty) (*walletrpc.LightdInfo, error) {
	return common.GetLightdInfo()
}

// maxRawTxSize bounds the raw transaction bytes lightwalletd will forward to
// zcashd. A Zcash transaction cannot exceed the 2,000,000-byte block size
// limit, so anything larger is unminable by definition and there is no reason
// to spend memory expanding it or to make the backend parse it.
const maxRawTxSize = 2000000

// SendTransaction forwards raw transaction bytes to a zcashd instance over JSON-RPC
func (s *lwdStreamer) SendTransaction(ctx context.Context, rawtx *walletrpc.RawTransaction) (*walletrpc.SendResponse, error) {
	common.Log.Debugf("gRPC SendTransaction(%+v)\n", rawtx)
	// sendrawtransaction "hexstring" ( allowhighfees )
	//
	// Submits raw transaction (binary) to local node and network.
	//
	// Result:
	// "hex"             (string) The transaction hash in hex

	// Verify rawtx
	if rawtx == nil || rawtx.Data == nil {
		return nil, errors.New("Bad transaction data")
	}
	// Reject an oversized transaction before expanding it: the bytes below are
	// hex-encoded (doubling them) and JSON-marshalled before zcashd ever sees
	// them, so without this an unauthenticated client can force large
	// allocations here and parsing work in the backend with a transaction that
	// can never be mined (GHSA-6ppp-r2gc-9q6v).
	if len(rawtx.Data) > maxRawTxSize {
		return nil, status.Errorf(codes.InvalidArgument,
			"SendTransaction: transaction is too large: %d bytes (limit %d)",
			len(rawtx.Data), maxRawTxSize)
	}

	// Construct raw JSON-RPC params
	params := make([]json.RawMessage, 1)
	txJSON, err := json.Marshal(hex.EncodeToString(rawtx.Data))
	if err != nil {
		return &walletrpc.SendResponse{}, err
	}
	params[0] = txJSON
	result, rpcErr := common.RawRequest(ctx, "sendrawtransaction", params)

	var errCode int64
	var errMsg string

	// For some reason, the error responses are not JSON
	if rpcErr != nil {
		errParts := strings.SplitN(rpcErr.Error(), ":", 2)
		if len(errParts) < 2 {
			return nil, errors.New("SendTransaction couldn't parse error code")
		}
		errMsg = strings.TrimSpace(errParts[1])
		errCode, err = strconv.ParseInt(errParts[0], 10, 32)
		if err != nil {
			// This should never happen. We can't panic here, but it's that class of error.
			// This is why we need integration testing to work better than regtest currently does. TODO.
			return nil, errors.New("SendTransaction couldn't parse error code")
		}
	} else {
		errMsg = string(result)
	}

	// TODO these are called Error but they aren't at the moment.
	// A success will return code 0 and message txhash.
	r := &walletrpc.SendResponse{
		ErrorCode:    int32(errCode),
		ErrorMessage: errMsg,
	}
	common.Log.Tracef("  return: %+v\n", r)
	return r, nil
}

func getTaddressBalanceZcashdRpc(ctx context.Context, addressList []string) (*walletrpc.Balance, error) {
	for _, addr := range addressList {
		if err := checkTaddress(addr); err != nil {
			return &walletrpc.Balance{}, err
		}
	}
	params := make([]json.RawMessage, 1)
	addrList := &common.ZcashdRpcRequestGetaddressbalance{
		Addresses: addressList,
	}
	param, err := json.Marshal(addrList)
	if err != nil {
		return &walletrpc.Balance{}, err
	}
	params[0] = param

	result, rpcErr := common.RawRequest(ctx, "getaddressbalance", params)
	if rpcErr != nil {
		return &walletrpc.Balance{}, rpcErr
	}
	var balanceReply common.ZcashdRpcReplyGetaddressbalance
	err = json.Unmarshal(result, &balanceReply)
	if err != nil {
		return &walletrpc.Balance{}, err
	}
	return &walletrpc.Balance{ValueZat: balanceReply.Balance}, nil
}

// GetTaddressBalance returns the total balance for a list of taddrs
func (s *lwdStreamer) GetTaddressBalance(ctx context.Context, addresses *walletrpc.AddressList) (*walletrpc.Balance, error) {
	common.Log.Debugf("gRPC GetTaddressBalance(%+v)\n", addresses)
	r, err := getTaddressBalanceZcashdRpc(ctx, addresses.Addresses)
	if err == nil {
		common.Log.Tracef("  return: %+v\n", r)
	}
	return r, err
}

// maxTaddrsPerRequest bounds the number of transparent addresses a single
// request may cause lightwalletd to process, across the transparent-address
// gRPC methods. Without a cap, an unauthenticated client can drive unbounded
// memory growth and backend work: GetTaddressBalanceStream accumulates
// streamed addresses until the process is OOM-killed, and GetAddressUtxos
// forwards the whole list to zcashd and materializes the full result before
// applying client-side limits. The unary GetTaddressBalance is already
// implicitly bounded by gRPC's MaxRecvMsgSize; this gives the other methods an
// equivalent bound, generous for any legitimate wallet (GHSA-x4m7-3gpp-xc36).
const maxTaddrsPerRequest = 10000

// GetTaddressBalanceStream returns the total balance for a list of taddrs
func (s *lwdStreamer) GetTaddressBalanceStream(addresses walletrpc.CompactTxStreamer_GetTaddressBalanceStreamServer) error {
	common.Log.Debugf("gRPC GetTaddressBalanceStream(%+v)\n", addresses)
	addressList := make([]string, 0)
	for {
		addr, err := addresses.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// Validate and bound each address as it arrives, rather than
		// accumulating unbounded, unvalidated input (GHSA-x4m7-3gpp-xc36).
		if err := checkTaddress(addr.Address); err != nil {
			return err
		}
		if len(addressList) >= maxTaddrsPerRequest {
			return status.Errorf(codes.ResourceExhausted,
				"GetTaddressBalanceStream: too many addresses (limit %d)", maxTaddrsPerRequest)
		}
		addressList = append(addressList, addr.Address)
	}
	balance, err := getTaddressBalanceZcashdRpc(addresses.Context(), addressList)
	if err != nil {
		return err
	}
	addresses.SendAndClose(balance)
	common.Log.Tracef("  return: %+v\n", balance)
	return nil
}

func (s *lwdStreamer) GetMempoolStream(_empty *walletrpc.Empty, resp walletrpc.CompactTxStreamer_GetMempoolStreamServer) error {
	common.Log.Debugf("gRPC GetMempoolStream()\n")
	err := common.GetMempool(resp.Context(), func(tx *walletrpc.RawTransaction) error {
		return resp.Send(tx)
	})
	return err
}

func getAddressUtxos(ctx context.Context, arg *walletrpc.GetAddressUtxosArg, f func(*walletrpc.GetAddressUtxosReply) error) error {
	// Bound the address count before contacting zcashd: getaddressutxos cannot
	// push down StartHeight/MaxEntries, so lightwalletd fetches and
	// materializes the entire backend result before applying those limits.
	// Capping the input keeps one request from forcing unbounded backend work
	// and result materialization (GHSA-x4m7-3gpp-xc36).
	if len(arg.Addresses) > maxTaddrsPerRequest {
		return status.Errorf(codes.ResourceExhausted,
			"getAddressUtxos: too many addresses (limit %d)", maxTaddrsPerRequest)
	}
	for _, a := range arg.Addresses {
		if err := checkTaddress(a); err != nil {
			return err
		}
	}
	params := make([]json.RawMessage, 1)
	addrList := &common.ZcashdRpcRequestGetaddressutxos{
		Addresses: arg.Addresses,
	}
	param, err := json.Marshal(addrList)
	if err != nil {
		return err
	}
	params[0] = param
	result, rpcErr := common.RawRequest(ctx, "getaddressutxos", params)
	if rpcErr != nil {
		return rpcErr
	}
	var utxosReply []common.ZcashdRpcReplyGetaddressutxos
	err = json.Unmarshal(result, &utxosReply)
	if err != nil {
		return err
	}
	n := 0
	for _, utxo := range utxosReply {
		if uint64(utxo.Height) < arg.StartHeight {
			continue
		}
		n++
		if arg.MaxEntries > 0 && uint32(n) > arg.MaxEntries {
			break
		}
		txidBytes, err := hex.DecodeString(utxo.Txid)
		if err != nil {
			return err
		}
		scriptBytes, err := hex.DecodeString(utxo.Script)
		if err != nil {
			return err
		}
		err = f(&walletrpc.GetAddressUtxosReply{
			Address:  utxo.Address,
			Txid:     parser.Reverse(txidBytes),
			Index:    int32(utxo.OutputIndex),
			Script:   scriptBytes,
			ValueZat: int64(utxo.Satoshis),
			Height:   uint64(utxo.Height),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *lwdStreamer) GetAddressUtxos(ctx context.Context, arg *walletrpc.GetAddressUtxosArg) (*walletrpc.GetAddressUtxosReplyList, error) {
	common.Log.Debugf("gRPC GetAddressUtxos(%+v)\n", arg)
	addressUtxos := make([]*walletrpc.GetAddressUtxosReply, 0)
	err := getAddressUtxos(ctx, arg, func(utxo *walletrpc.GetAddressUtxosReply) error {
		addressUtxos = append(addressUtxos, utxo)
		return nil
	})
	if err != nil {
		return &walletrpc.GetAddressUtxosReplyList{}, err
	}
	r := &walletrpc.GetAddressUtxosReplyList{AddressUtxos: addressUtxos}
	common.Log.Tracef("  return: %+v\n", r)
	return r, nil
}

func (s *lwdStreamer) GetAddressUtxosStream(arg *walletrpc.GetAddressUtxosArg, resp walletrpc.CompactTxStreamer_GetAddressUtxosStreamServer) error {
	common.Log.Debugf("gRPC GetAddressUtxosStream(%+v)\n", arg)
	err := getAddressUtxos(resp.Context(), arg, func(utxo *walletrpc.GetAddressUtxosReply) error {
		return resp.Send(utxo)
	})
	if err != nil {
		return err
	}
	return nil
}

// This rpc is used only for testing.
var concurrent int64

func (s *lwdStreamer) Ping(ctx context.Context, in *walletrpc.Duration) (*walletrpc.PingResponse, error) {
	// This gRPC allows the client to create an arbitrary number of
	// concurrent threads, which could run the server out of resources,
	// so only allow if explicitly enabled.
	if !s.pingEnable {
		return nil, errors.New("Ping not enabled, start lightwalletd with --ping-very-insecure")
	}
	var response walletrpc.PingResponse
	response.Entry = atomic.AddInt64(&concurrent, 1)
	time.Sleep(time.Duration(in.IntervalUs) * time.Microsecond)
	response.Exit = atomic.AddInt64(&concurrent, -1)
	return &response, nil
}

// SetMetaState lets the test driver control some GetLightdInfo values.
func (s *DarksideStreamer) Reset(ctx context.Context, ms *walletrpc.DarksideMetaState) (*walletrpc.Empty, error) {
	match, err := regexp.Match("\\A[a-fA-F0-9]+\\z", []byte(ms.BranchID))
	if err != nil || !match {
		return nil, errors.New("Invalid branch ID")
	}

	match, err = regexp.Match("\\A[a-zA-Z0-9]+\\z", []byte(ms.ChainName))
	if err != nil || !match {
		return nil, errors.New("Invalid chain name")
	}
	err = common.DarksideReset(int(ms.SaplingActivation), ms.BranchID, ms.ChainName)
	if err != nil {
		return nil, err
	}
	return &walletrpc.Empty{}, nil
}

// StageBlocksStream accepts a list of blocks from the wallet test code,
// and makes them available to present from the mock zcashd's GetBlock rpc.
func (s *DarksideStreamer) StageBlocksStream(blocks walletrpc.DarksideStreamer_StageBlocksStreamServer) error {
	for {
		b, err := blocks.Recv()
		if err == io.EOF {
			blocks.SendAndClose(&walletrpc.Empty{})
			return nil
		}
		if err != nil {
			return err
		}
		common.DarksideStageBlockStream(b.Block)
	}
}

// StageBlocks loads blocks from the given URL to the staging area.
func (s *DarksideStreamer) StageBlocks(ctx context.Context, u *walletrpc.DarksideBlocksURL) (*walletrpc.Empty, error) {
	if err := common.DarksideStageBlocks(u.Url); err != nil {
		return nil, err
	}
	return &walletrpc.Empty{}, nil
}

// StageBlocksCreate stages a set of synthetic (manufactured on the fly) blocks.
func (s *DarksideStreamer) StageBlocksCreate(ctx context.Context, e *walletrpc.DarksideEmptyBlocks) (*walletrpc.Empty, error) {
	if err := common.DarksideStageBlocksCreate(e.Height, e.Nonce, e.Count); err != nil {
		return nil, err
	}
	return &walletrpc.Empty{}, nil
}

// StageTransactionsStream adds the given transactions to the staging area.
func (s *DarksideStreamer) StageTransactionsStream(tx walletrpc.DarksideStreamer_StageTransactionsStreamServer) error {
	// My current thinking is that this should take a JSON array of {height, txid}, store them,
	// then DarksideAddBlock() would "inject" transactions into blocks as its storing
	// them (remembering to update the header so the block hash changes).
	for {
		transaction, err := tx.Recv()
		if err == io.EOF {
			tx.SendAndClose(&walletrpc.Empty{})
			return nil
		}
		if err != nil {
			return err
		}
		err = common.DarksideStageTransaction(int(transaction.Height), transaction.Data)
		if err != nil {
			return err
		}
	}
}

// StageTransactions loads blocks from the given URL to the staging area.
func (s *DarksideStreamer) StageTransactions(ctx context.Context, u *walletrpc.DarksideTransactionsURL) (*walletrpc.Empty, error) {
	if err := common.DarksideStageTransactionsURL(int(u.Height), u.Url); err != nil {
		return nil, err
	}
	return &walletrpc.Empty{}, nil
}

// ApplyStaged merges all staged transactions into staged blocks and all staged blocks into the active blockchain.
func (s *DarksideStreamer) ApplyStaged(ctx context.Context, h *walletrpc.DarksideHeight) (*walletrpc.Empty, error) {
	return &walletrpc.Empty{}, common.DarksideApplyStaged(int(h.Height))
}

// GetIncomingTransactions returns the transactions that were submitted via SendTransaction().
func (s *DarksideStreamer) GetIncomingTransactions(in *walletrpc.Empty, resp walletrpc.DarksideStreamer_GetIncomingTransactionsServer) error {
	// Get all of the incoming transactions we're received via SendTransaction()
	for _, txBytes := range common.DarksideGetIncomingTransactions() {
		err := resp.Send(&walletrpc.RawTransaction{Data: txBytes, Height: 0})
		if err != nil {
			return err
		}
	}
	return nil
}

// ClearIncomingTransactions empties the incoming transaction list.
func (s *DarksideStreamer) ClearIncomingTransactions(ctx context.Context, e *walletrpc.Empty) (*walletrpc.Empty, error) {
	common.DarksideClearIncomingTransactions()
	return &walletrpc.Empty{}, nil
}

// AddAddressUtxo adds a UTXO which will be returned by GetAddressUtxos() (above)
func (s *DarksideStreamer) AddAddressUtxo(ctx context.Context, arg *walletrpc.GetAddressUtxosReply) (*walletrpc.Empty, error) {
	utxosReply := common.ZcashdRpcReplyGetaddressutxos{
		Address:     arg.Address,
		Txid:        hex.EncodeToString(parser.Reverse(arg.Txid)),
		OutputIndex: int64(arg.Index),
		Script:      hex.EncodeToString(arg.Script),
		Satoshis:    uint64(arg.ValueZat),
		Height:      int(arg.Height),
	}
	err := common.DarksideAddAddressUtxo(utxosReply)
	return &walletrpc.Empty{}, err
}

// ClearAddressUtxo removes the list of cached utxo entries
func (s *DarksideStreamer) ClearAddressUtxo(ctx context.Context, arg *walletrpc.Empty) (*walletrpc.Empty, error) {
	err := common.DarksideClearAddressUtxos()
	return &walletrpc.Empty{}, err
}
