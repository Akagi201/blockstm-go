package main

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
)

type BlockInfo struct {
	Uncle  string
	Parent string

	Hash         string
	Number       uint64
	Nonce        uint64
	Transactions []*TransactionInfo

	ReceiveAt time.Time
	Time      uint64

	Difficulty uint64
	Size       uint64
	GasUsed    uint64
	GasLimit   uint64
	BaseFeeGas uint64
	ExtraData  []byte
}
type TransactionInfo struct {
	ChainId    *big.Int
	Data       []byte
	AccessList types.AccessList
	Gas        uint64
	GasPrice   *big.Int
	Value      *big.Int
	Nonce      uint64
	From       string
	To         string
	Cost       *big.Int
	Hash       string
	Time       time.Time
	Logs       []*LogInfo
}

type LogInfo struct {
	Address     string
	Topics      []string
	Data        []byte
	Index       uint
	BlockNumber uint64
	BlockHash   string
	TxHash      string
	TxIndex     uint
}
