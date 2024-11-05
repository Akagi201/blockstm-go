package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
)

func fetchOptimismBlocks() {
	// get from env OP_MAINNET_RPC
	rpcURL := os.Getenv("OP_MAINNET_RPC")
	if rpcURL == "" {
		panic("OP_MAINNET_RPC env var is not set")
	}

	// https://docs.optimism.io/builders/node-operators/network-upgrades
	blockNumbers := []int64{
		127609270,
	}

	rpcClient, err := Dial(rpcURL, "../../tests/data/optimism")
	if err != nil {
		panic(err)
	}
	defer rpcClient.Close()

	for _, blockNumber := range blockNumbers {
		// get block by number
		var bigInt big.Int
		bigInt.SetInt64(blockNumber)
		err := rpcClient.BlockByNumber(context.Background(), &bigInt)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Written block data, block number: %d\n", blockNumber)

		err = rpcClient.ExecutionWitness(context.Background(), &bigInt)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Written witness data, block number: %d\n", blockNumber)
	}
}

func main() {
	fetchOptimismBlocks()
}
