package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type EthRPC struct {
	Rpc    string
	Ctx    context.Context
	client *ethclient.Client
}

func NewEthRPC(rpc string) *EthRPC {
	client, err := ethclient.Dial(rpc)
	if err != nil {
		log.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}
	return &EthRPC{
		Rpc:    rpc,
		Ctx:    context.Background(),
		client: client,
	}
}

func (rpc *EthRPC) Transaction2Schema(tx *types.Transaction) *TransactionInfo {
	var to string
	if tx.To() != nil {
		to = tx.To().Hex()
	}
	return &TransactionInfo{
		ChainId:    tx.ChainId(),
		Hash:       tx.Hash().Hex(),
		Nonce:      tx.Nonce(),
		To:         to,
		Value:      tx.Value(),
		Gas:        tx.Gas(),
		GasPrice:   tx.GasPrice(),
		Data:       tx.Data(),
		AccessList: tx.AccessList(),
		Cost:       tx.Cost(),
		Time:       tx.Time(),
	}
}

func (rpc *EthRPC) Block2Schema(block *types.Block) *BlockInfo {
	txs := make([]*TransactionInfo, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		txs[i] = rpc.Transaction2Schema(tx)
	}

	var baseFeeGas uint64
	if block.BaseFee() != nil {
		baseFeeGas = block.BaseFee().Uint64()
	}

	return &BlockInfo{
		Uncle:        block.UncleHash().Hex(),
		Parent:       block.ParentHash().Hex(),
		Hash:         block.Hash().Hex(),
		Number:       block.NumberU64(),
		Nonce:        block.Nonce(),
		Transactions: txs,
		ReceiveAt:    block.ReceivedAt,
		Time:         block.Time(),
		Difficulty:   block.Difficulty().Uint64(),
		Size:         block.Size(),
		GasUsed:      block.GasUsed(),
		GasLimit:     block.GasLimit(),
		BaseFeeGas:   baseFeeGas,
		ExtraData:    block.Header().Extra,
	}
}

func (rpc *EthRPC) GetBlockByNumber(blockNumber *big.Int) (*BlockInfo, error) {
	block, err := rpc.client.BlockByNumber(rpc.Ctx, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block %v: %v", blockNumber, err)
	}
	return rpc.Block2Schema(block), nil
}

func fetchMainnetBlocks() {
	rpcURL := "https://ethereum-rpc.publicnode.com"

	blockNumbers := []int64{
		46147,    // FRONTIER
		1150000,  // HOMESTEAD
		2463002,  // TANGERINE
		2675000,  // SPURIOUS_DRAGON
		4370003,  // BYZANTIUM
		7280003,  // PETERSBURG
		9069001,  // ISTANBUL
		12244002, // BERLIN
		12965034, // LONDON
		15537395, // MERGE
		17035010, // SHANGHAI
		19426587, // CANCUN
	}

	ethRPC := NewEthRPC(rpcURL)

	for _, blockNumber := range blockNumbers {
		blockInfo, err := ethRPC.GetBlockByNumber(big.NewInt(blockNumber))
		if err != nil {
			log.Fatalf("Failed to get block: %v", err)
		}

		dir := fmt.Sprintf("../../tests/data/mainnet/blocks/%d", blockNumber)
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Fatalf("Failed to create directory: %v", err)
		}

		blockFile, err := os.Create(filepath.Join(dir, "block.json"))
		if err != nil {
			log.Fatalf("Failed to create block file: %v", err)
		}
		defer blockFile.Close()

		err = json.NewEncoder(blockFile).Encode(blockInfo)
		if err != nil {
			log.Fatalf("Failed to write block to file: %v", err)
		}

		stateFile, err := os.Create(filepath.Join(dir, "pre_state.json"))
		if err != nil {
			log.Fatalf("Failed to create state file: %v", err)
		}
		defer stateFile.Close()

		// err = json.NewEncoder(stateFile).Encode(rpcStorage.GetCache())
		// if err != nil {
		//     log.Fatalf("Failed to write state to file: %v", err)
		// }
	}
}

func fetchOptimismBlocks() {
	rpcURL := "https://optimism-rpc.publicnode.com"

	// https://docs.optimism.io/builders/node-operators/network-upgrades
	blockNumbers := []int64{
		125037129,
	}

	ethRPC := NewEthRPC(rpcURL)

	for _, blockNumber := range blockNumbers {
		blockInfo, err := ethRPC.GetBlockByNumber(big.NewInt(blockNumber))
		if err != nil {
			log.Fatalf("Failed to get block: %v", err)
		}

		dir := fmt.Sprintf("../../tests/data/optimism/blocks/%d", blockNumber)
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Fatalf("Failed to create directory: %v", err)
		}

		blockFile, err := os.Create(filepath.Join(dir, "block.json"))
		if err != nil {
			log.Fatalf("Failed to create block file: %v", err)
		}
		defer blockFile.Close()

		err = json.NewEncoder(blockFile).Encode(blockInfo)
		if err != nil {
			log.Fatalf("Failed to write block to file: %v", err)
		}

		stateFile, err := os.Create(filepath.Join(dir, "pre_state.json"))
		if err != nil {
			log.Fatalf("Failed to create state file: %v", err)
		}
		defer stateFile.Close()

		// err = json.NewEncoder(stateFile).Encode(rpcStorage.GetCache())
		// if err != nil {
		//     log.Fatalf("Failed to write state to file: %v", err)
		// }
	}
}

func main() {
	fetchMainnetBlocks()
	fetchOptimismBlocks()
}
