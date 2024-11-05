package blockstm

import (
	"runtime"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/beacon"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/stateless"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

type StatelessExecutor struct {
	config  *params.ChainConfig
	block   *types.Block
	witness *stateless.Witness
}

func NewStatelessExecutor(config *params.ChainConfig, block *types.Block, witness *stateless.Witness) *StatelessExecutor {
	return &StatelessExecutor{
		config:  config,
		block:   block,
		witness: witness,
	}
}

func (exec *StatelessExecutor) Process(parallel bool) (common.Hash, common.Hash, error) {
	// Sanity check if the supplied block accidentally contains a set root or
	// receipt hash. If so, be very loud, but still continue.
	if exec.block.Root() != (common.Hash{}) {
		log.Error("stateless runner received state root it's expected to calculate (faulty consensus client)", "block", exec.block.Number())
	}
	if exec.block.ReceiptHash() != (common.Hash{}) {
		log.Error("stateless runner received receipt root it's expected to calculate (faulty consensus client)", "block", exec.block.Number())
	}
	// Create and populate the state database to serve as the stateless backend
	memdb := exec.witness.MakeHashDB()
	db, err := state.New(exec.witness.Root(), state.NewDatabase(triedb.NewDatabase(memdb, triedb.HashDefaults), nil))
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	chain, err := core.NewHeaderChain(memdb, exec.config, beacon.New(ethash.NewFaker()), nil)
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	var processor core.Processor
	if parallel {
		parallel := runtime.NumCPU()
		processor = NewParallelStateProcessor(exec.config, chain, parallel)
	} else {
		processor = core.NewStateProcessor(exec.config, chain)
	}
	validator := core.NewBlockValidator(exec.config, nil) // No chain, we only validate the state, not the block

	// Run the stateless blocks processing and self-validate certain fields
	res, err := processor.Process(exec.block, db, vm.Config{})
	if err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	if err = validator.ValidateState(exec.block, db, res, true); err != nil {
		return common.Hash{}, common.Hash{}, err
	}
	// Almost everything validated, but receipt and state root needs to be returned
	receiptRoot := types.DeriveSha(res.Receipts, trie.NewStackTrie(nil))
	stateRoot := db.IntermediateRoot(exec.config.IsEIP158(exec.block.Number()))

	return receiptRoot, stateRoot, nil
}
