package blockstm

import (
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

// SerialBlockExecutor executes a block serially.
type SerialBlockExecutor struct {
	config *params.ChainConfig // Chain configuration options
	chain  *core.HeaderChain   // Canonical header chain
}

func NewSerialBlockExecutor(config *params.ChainConfig, chain *core.HeaderChain) *SerialBlockExecutor {
	return &SerialBlockExecutor{
		config: config,
		chain:  chain,
	}
}

// Process executes a block serially.
func (exec *SerialBlockExecutor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		gp          = new(core.GasPool).AddGas(block.GasLimit())
	)

	// Mutate the block and state according to any hard-fork specs
	if exec.config.DAOForkSupport && exec.config.DAOForkBlock != nil && exec.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	var (
		context vm.BlockContext
		signer  = types.MakeSigner(exec.config, header.Number, header.Time)
	)
	context = core.NewEVMBlockContext(header, exec.chain, nil, exec.config, statedb)
	vmenv := vm.NewEVM(context, vm.TxContext{}, statedb, exec.config, cfg)
	if beaconRoot := block.BeaconRoot(); beaconRoot != nil {
		core.ProcessBeaconBlockRoot(*beaconRoot, vmenv, statedb)
	}
	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		msg, err := core.TransactionToMessage(tx, signer, header.BaseFee)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		statedb.SetTxContext(tx.Hash(), i)

		receipt, err := core.ApplyTransactionWithEVM(msg, exec.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
	}
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !exec.config.IsShanghai(block.Number(), block.Time()) {
		return nil, nil, 0, errors.New("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	exec.chain.Engine().Finalize(exec.chain, header, statedb, block.Body())

	return receipts, allLogs, *usedGas, nil
}

// ParallelBlockExecutor executes a block serially.
type ParallelBlockExecutor struct {
	config *params.ChainConfig // Chain configuration options
	chain  *core.HeaderChain   // Canonical header chain
}

// NewParallelBlockExecutor creates a new ParallelBlockExecutor.
func NewParallelBlockExecutor(config *params.ChainConfig, chain *core.HeaderChain) *ParallelBlockExecutor {
	return &ParallelBlockExecutor{
		config: config,
		chain:  chain,
	}
}

// Process executes a block serially.
func (exec *ParallelBlockExecutor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	// TODO
	return nil, nil, 0, nil
}
