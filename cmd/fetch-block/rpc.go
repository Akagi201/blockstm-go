package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

type RawRpcClient struct {
	c          *rpc.Client
	outputPath string
}

func Dial(rawurl string, path string) (*RawRpcClient, error) {
	return DialContext(context.Background(), rawurl, path)
}

func DialContext(ctx context.Context, rawurl string, path string) (*RawRpcClient, error) {
	c, err := rpc.DialContext(ctx, rawurl)
	if err != nil {
		return nil, err
	}
	return NewRawRpcClient(c, path), nil
}

func NewRawRpcClient(c *rpc.Client, path string) *RawRpcClient {
	return &RawRpcClient{c, path}
}

func (ec *RawRpcClient) Close() {
	ec.c.Close()
}

func (ec *RawRpcClient) BlockByNumber(ctx context.Context, number *big.Int) error {
	var raw json.RawMessage
	err := ec.c.CallContext(ctx, &raw, "eth_getBlockByNumber", toBlockNumArg(number), true)
	if err != nil {
		return err
	}
	// write raw to file outputPath/<block-number>/block.json
	blockDir := filepath.Join(ec.outputPath, fmt.Sprintf("%d", number))
	err = os.MkdirAll(blockDir, 0755)
	if err != nil {
		return err
	}
	blockFile := filepath.Join(blockDir, "block.json")
	err = os.WriteFile(blockFile, raw, 0644)
	return err
}

func (ec *RawRpcClient) ExecutionWitness(ctx context.Context, number *big.Int) error {
	var raw json.RawMessage
	err := ec.c.CallContext(ctx, &raw, "debug_executionWitness", toBlockNumArg(number))
	if err != nil {
		return err
	}
	// write raw to file outputPath/<block-number>/witness.json
	witnessDir := filepath.Join(ec.outputPath, fmt.Sprintf("%d", number))
	err = os.MkdirAll(witnessDir, 0755)
	if err != nil {
		return err
	}
	witnessFile := filepath.Join(witnessDir, "witness.json")
	err = os.WriteFile(witnessFile, raw, 0644)
	return err
}

func (ec *RawRpcClient) ParseBlock(number uint64) (*types.Block, error) {
	// read raw from file outputPath/<block-number>/block.json
	blockDir := filepath.Join(ec.outputPath, fmt.Sprintf("%d", number))
	blockFile := filepath.Join(blockDir, "block.json")
	data, err := os.ReadFile(blockFile)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}

	// Decode header and transactions.
	var head *types.Header
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, err
	}
	// When the block is not found, the API returns JSON null.
	if head == nil {
		return nil, ethereum.NotFound
	}

	var body rpcBlock
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	// Quick-verify transaction and uncle lists. This mostly helps with debugging the server.
	if head.UncleHash == types.EmptyUncleHash && len(body.UncleHashes) > 0 {
		return nil, errors.New("server returned non-empty uncle list but block header indicates no uncles")
	}
	if head.UncleHash != types.EmptyUncleHash && len(body.UncleHashes) == 0 {
		return nil, errors.New("server returned empty uncle list but block header indicates uncles")
	}
	if head.TxHash == types.EmptyTxsHash && len(body.Transactions) > 0 {
		return nil, errors.New("server returned non-empty transaction list but block header indicates no transactions")
	}
	if head.TxHash != types.EmptyTxsHash && len(body.Transactions) == 0 {
		return nil, errors.New("server returned empty transaction list but block header indicates transactions")
	}
	// Load uncles because they are not included in the block response.
	var uncles []*types.Header
	if len(body.UncleHashes) > 0 {
		uncles = make([]*types.Header, len(body.UncleHashes))
		reqs := make([]rpc.BatchElem, len(body.UncleHashes))
		for i := range reqs {
			reqs[i] = rpc.BatchElem{
				Method: "eth_getUncleByBlockHashAndIndex",
				Args:   []interface{}{body.Hash, hexutil.EncodeUint64(uint64(i))},
				Result: &uncles[i],
			}
		}
		if err := ec.c.BatchCallContext(context.Background(), reqs); err != nil {
			return nil, err
		}
		for i := range reqs {
			if reqs[i].Error != nil {
				return nil, reqs[i].Error
			}
			if uncles[i] == nil {
				return nil, fmt.Errorf("got null header for uncle %d of block %x", i, body.Hash[:])
			}
		}
	}
	// Fill the sender cache of transactions in the block.
	txs := make([]*types.Transaction, len(body.Transactions))
	for i, tx := range body.Transactions {
		if tx.From != nil {
			setSenderFromServer(tx.tx, *tx.From, body.Hash)
		}
		txs[i] = tx.tx
	}
	return types.NewBlockWithHeader(head).WithBody(
		types.Body{
			Transactions: txs,
			Uncles:       uncles,
			Withdrawals:  body.Withdrawals,
			Requests:     body.Requests,
		}), nil
}

func (ec *RawRpcClient) ParseExecutionWitness(number uint64) (*stateless.ExecutionWitness, error) {
	// read raw from file outputPath/<block-number>/witness.json
	witnessDir := filepath.Join(ec.outputPath, fmt.Sprintf("%d", number))
	witnessFile := filepath.Join(witnessDir, "witness.json")
	data, err := os.ReadFile(witnessFile)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = json.Unmarshal(data, &raw)
	if err != nil {
		return nil, err
	}
	var witness stateless.ExecutionWitness
	err = json.Unmarshal(raw, &witness)
	if err != nil {
		return nil, err
	}
	return &witness, nil
}

func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	if number.Sign() >= 0 {
		return hexutil.EncodeBig(number)
	}
	// It's negative.
	if number.IsInt64() {
		return rpc.BlockNumber(number.Int64()).String()
	}
	// It's negative and large, which is invalid.
	return fmt.Sprintf("<invalid %d>", number)
}

type rpcBlock struct {
	Hash         common.Hash         `json:"hash"`
	Transactions []rpcTransaction    `json:"transactions"`
	UncleHashes  []common.Hash       `json:"uncles"`
	Withdrawals  []*types.Withdrawal `json:"withdrawals,omitempty"`
	Requests     []*types.Request    `json:"requests,omitempty"`
}

type rpcTransaction struct {
	tx *types.Transaction
	txExtraInfo
}

type txExtraInfo struct {
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

func (tx *rpcTransaction) UnmarshalJSON(msg []byte) error {
	if err := json.Unmarshal(msg, &tx.tx); err != nil {
		return err
	}
	return json.Unmarshal(msg, &tx.txExtraInfo)
}
