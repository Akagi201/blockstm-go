package blockstm

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
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

const BalancePath = 1
const NoncePath = 2
const CodePath = 3
const SuicidePath = 4

type MvCache struct {
	mvHashmap    *MVHashMap
	incarnation  int
	readMap      map[Key]ReadDescriptor
	writeMap     map[Key]WriteDescriptor
	revertedKeys map[Key]struct{}
	dep          int
}

func (m *MvCache) SetMVHashmap(mvhm *MVHashMap) {
	m.mvHashmap = mvhm
	m.dep = -1
}

func (m *MvCache) GetMVHashmap() *MVHashMap {
	return m.mvHashmap
}

func (m *MvCache) MVWriteList() []WriteDescriptor {
	writes := make([]WriteDescriptor, 0, len(m.writeMap))

	for _, v := range m.writeMap {
		if _, ok := m.revertedKeys[v.Path]; !ok {
			writes = append(writes, v)
		}
	}

	return writes
}

func (m *MvCache) MVFullWriteList() []WriteDescriptor {
	writes := make([]WriteDescriptor, 0, len(m.writeMap))

	for _, v := range m.writeMap {
		writes = append(writes, v)
	}

	return writes
}

func (m *MvCache) MVReadMap() map[Key]ReadDescriptor {
	return m.readMap
}

func (m *MvCache) MVReadList() []ReadDescriptor {
	reads := make([]ReadDescriptor, 0, len(m.readMap))

	for _, v := range m.MVReadMap() {
		reads = append(reads, v)
	}

	return reads
}

func (m *MvCache) ensureReadMap() {
	if m.readMap == nil {
		m.readMap = make(map[Key]ReadDescriptor)
	}
}

func (m *MvCache) ensureWriteMap() {
	if m.writeMap == nil {
		m.writeMap = make(map[Key]WriteDescriptor)
	}
}

func (m *MvCache) ClearReadMap() {
	m.readMap = make(map[Key]ReadDescriptor)
}

func (m *MvCache) ClearWriteMap() {
	m.writeMap = make(map[Key]WriteDescriptor)
}

func (m *MvCache) HadInvalidRead() bool {
	return m.dep >= 0
}

func (m *MvCache) DepTxIndex() int {
	return m.dep
}

func (m *MvCache) SetIncarnation(inc int) {
	m.incarnation = inc
}

type ExecutionTask struct {
	msg    core.Message
	config *params.ChainConfig

	gasLimit                   uint64
	blockNumber                *big.Int
	blockHash                  common.Hash
	tx                         *types.Transaction
	index                      int
	statedb                    *state.StateDB
	mvCache                    *MvCache
	header                     *types.Header
	blockChain                 *core.HeaderChain
	evmConfig                  vm.Config
	result                     *core.ExecutionResult
	shouldDelayFeeCal          *bool
	shouldRerunWithoutFeeDelay bool
	sender                     common.Address
	totalUsedGas               *uint64
	receipts                   *types.Receipts
	allLogs                    *[]*types.Log

	// length of dependencies          -> 2 + k (k = a whole number)
	// first 2 element in dependencies -> transaction index, and flag representing if delay is allowed or not
	//                                       (0 -> delay is not allowed, 1 -> delay is allowed)
	// next k elements in dependencies -> transaction indexes on which transaction i is dependent on
	dependencies []int
	coinbase     common.Address
	blockContext vm.BlockContext
}

// NewEVMTxContext creates a new transaction context for a single transaction.
func NewEVMTxContext(msg *core.Message) vm.TxContext {
	ctx := vm.TxContext{
		Origin:     msg.From,
		GasPrice:   new(big.Int).Set(msg.GasPrice),
		BlobHashes: msg.BlobHashes,
	}
	if msg.BlobGasFeeCap != nil {
		ctx.BlobFeeCap = new(big.Int).Set(msg.BlobGasFeeCap)
	}
	return ctx
}

func (task *ExecutionTask) Execute(mvh *MVHashMap, incarnation int) (err error) {
	task.statedb = task.statedb.Copy()
	task.statedb.SetTxContext(task.tx.Hash(), task.index)
	task.mvCache.SetMVHashmap(mvh)
	task.mvCache.SetIncarnation(incarnation)

	evm := vm.NewEVM(task.blockContext, vm.TxContext{}, task.statedb, task.config, task.evmConfig)

	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(&task.msg)
	evm.Reset(txContext, task.statedb)

	defer func() {
		if r := recover(); r != nil {
			// In some pre-matured executions, EVM will panic. Recover from panic and retry the execution.
			log.Debug("Recovered from EVM failure.", "Error:", r)

			err = ErrExecAbortError{Dependency: task.mvCache.DepTxIndex()}

			return
		}
	}()

	// Apply the transaction to the current state (included in the env).
	if *task.shouldDelayFeeCal {
		task.result, err = core.ApplyMessage(evm, &task.msg, new(core.GasPool).AddGas(task.gasLimit))

		if task.result == nil || err != nil {
			return ErrExecAbortError{Dependency: task.mvCache.DepTxIndex(), OriginError: err}
		}

		reads := task.mvCache.MVReadMap()

		if _, ok := reads[NewSubpathKey(task.blockContext.Coinbase, BalancePath)]; ok {
			log.Info("Coinbase is in MVReadMap", "address", task.blockContext.Coinbase)

			task.shouldRerunWithoutFeeDelay = true
		}

		// if _, ok := reads[blockstm.NewSubpathKey(task.result.BurntContractAddress, state.BalancePath)]; ok {
		// 	log.Info("BurntContractAddress is in MVReadMap", "address", task.result.BurntContractAddress)

		// 	task.shouldRerunWithoutFeeDelay = true
		// }
	} else {
		task.result, err = core.ApplyMessage(evm, &task.msg, new(core.GasPool).AddGas(task.gasLimit))
	}

	if task.mvCache.HadInvalidRead() || err != nil {
		err = ErrExecAbortError{Dependency: task.mvCache.DepTxIndex(), OriginError: err}
		return
	}

	task.statedb.Finalise(task.config.IsEIP158(task.blockNumber))

	return
}

func (task *ExecutionTask) MVReadList() []ReadDescriptor {
	return task.mvCache.MVReadList()
}

func (task *ExecutionTask) MVWriteList() []WriteDescriptor {
	return task.mvCache.MVWriteList()
}

func (task *ExecutionTask) MVFullWriteList() []WriteDescriptor {
	return task.mvCache.MVFullWriteList()
}

func (task *ExecutionTask) Sender() common.Address {
	return task.sender
}

func (task *ExecutionTask) Hash() common.Hash {
	return task.tx.Hash()
}

func (task *ExecutionTask) Dependencies() []int {
	return task.dependencies
}

func (task *ExecutionTask) Settle() {
	task.statedb.SetTxContext(task.tx.Hash(), task.index)

	for _, l := range task.statedb.GetLogs(task.tx.Hash(), task.blockNumber.Uint64(), task.blockHash) {
		task.statedb.AddLog(l)
	}

	for k, v := range task.statedb.Preimages() {
		task.statedb.AddPreimage(k, v)
	}

	// Update the state with pending changes.
	var root []byte

	if task.config.IsByzantium(task.blockNumber) {
		task.statedb.Finalise(true)
	} else {
		root = task.statedb.IntermediateRoot(task.config.IsEIP158(task.blockNumber)).Bytes()
	}

	*task.totalUsedGas += task.result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: task.tx.Type(), PostState: root, CumulativeGasUsed: *task.totalUsedGas}
	if task.result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}

	receipt.TxHash = task.tx.Hash()
	receipt.GasUsed = task.result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if task.msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(task.msg.From, task.tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = task.statedb.GetLogs(task.tx.Hash(), task.blockNumber.Uint64(), task.blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = task.blockHash
	receipt.BlockNumber = task.blockNumber
	receipt.TransactionIndex = uint(task.statedb.TxIndex())

	*task.receipts = append(*task.receipts, receipt)
	*task.allLogs = append(*task.allLogs, receipt.Logs...)
}

var parallelizabilityTimer = metrics.NewRegisteredTimer("block/parallelizability", nil)

// ParallelBlockExecutor executes a block serially.
type ParallelBlockExecutor struct {
	config            *params.ChainConfig // Chain configuration options
	chain             *core.HeaderChain   // Canonical header chain
	parallelProcesses int
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
	var (
		receipts    types.Receipts
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		usedGas     = new(uint64)
		metadata    = false
	)

	// Mutate the block and state according to any hard-fork specs
	if exec.config.DAOForkSupport && exec.config.DAOForkBlock != nil && exec.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}

	tasks := make([]ExecTask, 0, len(block.Transactions()))

	shouldDelayFeeCal := true

	coinbase, _ := exec.chain.Engine().Author(header)

	blockContext := core.NewEVMBlockContext(header, exec.chain, nil, exec.config, statedb)

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		msg, err := core.TransactionToMessage(tx, types.MakeSigner(exec.config, header.Number, header.Time), header.BaseFee)
		if err != nil {
			log.Error("error creating message", "err", err)
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		statedb := statedb.Copy()

		if msg.From == coinbase {
			shouldDelayFeeCal = false
		}

		task := &ExecutionTask{
			msg:               *msg,
			config:            exec.config,
			gasLimit:          block.GasLimit(),
			blockNumber:       blockNumber,
			blockHash:         blockHash,
			tx:                tx,
			index:             i,
			statedb:           statedb,
			blockChain:        exec.chain,
			header:            header,
			evmConfig:         cfg,
			shouldDelayFeeCal: &shouldDelayFeeCal,
			sender:            msg.From,
			totalUsedGas:      usedGas,
			receipts:          &receipts,
			allLogs:           &allLogs,
			coinbase:          coinbase,
			blockContext:      blockContext,
		}

		tasks = append(tasks, task)
	}

	backupStateDB := statedb.Copy()

	profile := false
	result, err := ExecuteParallel(nil, tasks, profile, metadata, exec.parallelProcesses)

	if err == nil && profile && result.Deps != nil {
		_, weight := result.Deps.LongestPath(result.Stats)

		serialWeight := uint64(0)

		for i := 0; i < len(result.Deps.GetVertices()); i++ {
			r, _ := result.Stats.Get(i)
			serialWeight += r.End - r.Start
		}

		parallelizabilityTimer.Update(time.Duration(serialWeight * 100 / weight))
	}

	for _, task := range tasks {
		task := task.(*ExecutionTask)
		if task.shouldRerunWithoutFeeDelay {
			shouldDelayFeeCal = false

			statedb.StopPrefetcher()
			*statedb = *backupStateDB

			allLogs = []*types.Log{}
			receipts = types.Receipts{}
			usedGas = new(uint64)

			for _, t := range tasks {
				t := t.(*ExecutionTask)
				t.statedb = backupStateDB
				t.allLogs = &allLogs
				t.receipts = &receipts
				t.totalUsedGas = usedGas
			}

			_, err = ExecuteParallel(nil, tasks, false, metadata, exec.parallelProcesses)

			break
		}
	}

	if err != nil {
		return nil, nil, 0, err
	}

	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	exec.chain.Engine().Finalize(exec.chain, header, statedb, block.Body())

	return receipts, allLogs, *usedGas, nil
}
