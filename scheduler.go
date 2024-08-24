package blockstm

import (
	"fmt"
	"sync"
	"time"

	"github.com/cornelk/hashmap"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type ParallelExecutionResult struct {
	TxIO    *TxnInputOutput
	Stats   *hashmap.Map[int, ExecutionStat] // map[int]ExecutionStat
	Deps    *DAG
	AllDeps map[int]map[int]bool
}

const numGoProcs = 1

type ParallelExecFailedError struct {
	Msg string
}

func (e ParallelExecFailedError) Error() string {
	return e.Msg
}

type ParallelExecutor struct {
	tasks []ExecTask

	// Stores the execution statistics for the last incarnation of each task
	// stats sync.Map // map[int]ExecutionStat
	stats *hashmap.Map[int, ExecutionStat]

	// Number of workers that execute transactions speculatively
	numSpeculativeProcs int

	// Channel for tasks that should be prioritized
	chTasks chan ExecVersionView

	// Channel for speculative tasks
	chSpeculativeTasks chan struct{}

	// Channel to signal that the result of a transaction could be written to storage
	specTaskQueue SafeQueue

	// A priority queue that stores speculative tasks
	chSettle chan int

	// Channel to signal that a transaction has finished executing
	chResults chan struct{}

	// A priority queue that stores the transaction index of results, so we can validate the results in order
	resultQueue SafeQueue

	// A wait group to wait for all settling tasks to finish
	settleWg sync.WaitGroup

	// An integer that tracks the index of last settled transaction
	lastSettled int

	// For a task that runs only after all of its preceding tasks have finished and passed validation,
	// its result will be absolutely valid and therefore its validation could be skipped.
	// This map stores the boolean value indicating whether a task satisfy this condition ( absolutely valid).
	skipCheck map[int]bool

	// Execution tasks stores the state of each execution task
	execTasks taskStatusManager

	// Validate tasks stores the state of each validation task
	validateTasks taskStatusManager

	// Stats for debugging purposes
	cntExec, cntSuccess, cntAbort, cntTotalValidations, cntValidationFail int

	diagExecSuccess, diagExecAbort []int

	// Multi-version hash map
	mvh *MVHashMap

	// Stores the inputs and outputs of the last incardanotion of all transactions
	lastTxIO *TxnInputOutput

	// Tracks the incarnation number of each transaction
	txIncarnations []int

	// A map that stores the estimated dependency of a transaction if it is aborted without any known dependency
	estimateDeps map[int][]int

	// A map that records whether a transaction result has been speculatively validated
	preValidated map[int]bool

	// Time records when the parallel execution starts
	begin time.Time

	// Enable profiling
	profile bool

	// Worker wait group
	workerWg sync.WaitGroup
}

func NewParallelExecutor(tasks []ExecTask, profile bool, metadata bool, numProcs int) *ParallelExecutor {
	numTasks := len(tasks)

	var resultQueue SafeQueue

	var specTaskQueue SafeQueue

	if metadata {
		resultQueue = NewSafeFIFOQueue(numTasks)
		specTaskQueue = NewSafeFIFOQueue(numTasks)
	} else {
		resultQueue = NewSafePriorityQueue(numTasks)
		specTaskQueue = NewSafePriorityQueue(numTasks)
	}

	pe := &ParallelExecutor{
		tasks:               tasks,
		stats: hashmap.NewSized[int, ExecutionStat](uintptr(numTasks)),
		numSpeculativeProcs: numProcs,
		chTasks:             make(chan ExecVersionView, numTasks),
		chSpeculativeTasks:  make(chan struct{}, numTasks),
		chSettle:            make(chan int, numTasks),
		chResults:           make(chan struct{}, numTasks),
		specTaskQueue:       specTaskQueue,
		resultQueue:         resultQueue,
		lastSettled:         -1,
		skipCheck:           make(map[int]bool),
		execTasks:           makeStatusManager(numTasks),
		validateTasks:       makeStatusManager(0),
		diagExecSuccess:     make([]int, numTasks),
		diagExecAbort:       make([]int, numTasks),
		mvh:                 MakeMVHashMap(),
		lastTxIO:            MakeTxnInputOutput(numTasks),
		txIncarnations:      make([]int, numTasks),
		estimateDeps:        make(map[int][]int),
		preValidated:        make(map[int]bool),
		begin:               time.Now(),
		profile:             profile,
	}

	return pe
}

// nolint: gocognit
func (pe *ParallelExecutor) Prepare() error {
	prevSenderTx := make(map[common.Address]int)

	for i, t := range pe.tasks {
		clearPendingFlag := false

		pe.skipCheck[i] = false
		pe.estimateDeps[i] = make([]int, 0)

		if len(t.Dependencies()) > 0 {
			for _, val := range t.Dependencies() {
				clearPendingFlag = true

				pe.execTasks.addDependencies(val, i)
			}

			if clearPendingFlag {
				pe.execTasks.clearPending(i)

				clearPendingFlag = false
			}
		} else {
			if tx, ok := prevSenderTx[t.Sender()]; ok {
				pe.execTasks.addDependencies(tx, i)
				pe.execTasks.clearPending(i)
			}

			prevSenderTx[t.Sender()] = i
		}
	}

	pe.workerWg.Add(pe.numSpeculativeProcs + numGoProcs)

	// Launch workers that execute transactions
	for i := 0; i < pe.numSpeculativeProcs+numGoProcs; i++ {
		go func(procNum int) {
			defer pe.workerWg.Done()

			doWork := func(task ExecVersionView) {
				start := time.Duration(0)
				if pe.profile {
					start = time.Since(pe.begin)
				}

				res := task.Execute()

				if res.err == nil {
					pe.mvh.FlushMVWriteSet(res.txAllOut)
				}

				pe.resultQueue.Push(res.ver.TxnIndex, res)
				pe.chResults <- struct{}{}

				if pe.profile {
					end := time.Since(pe.begin)
					pe.stats.Insert(res.ver.TxnIndex, ExecutionStat{
						TxIdx:       res.ver.TxnIndex,
						Incarnation: res.ver.Incarnation,
						Start:       uint64(start),
						End:         uint64(end),
						Worker:      procNum,
					})
				}
			}

			if procNum < pe.numSpeculativeProcs {
				for range pe.chSpeculativeTasks {
					doWork(pe.specTaskQueue.Pop().(ExecVersionView))
				}
			} else {
				for task := range pe.chTasks {
					doWork(task)
				}
			}
		}(i)
	}

	pe.settleWg.Add(1)

	go func() {
		for t := range pe.chSettle {
			pe.tasks[t].Settle()
		}

		pe.settleWg.Done()
	}()

	// bootstrap first execution
	tx := pe.execTasks.takeNextPending()

	if tx == -1 {
		return ParallelExecFailedError{"no executable transactions due to bad dependency"}
	}

	pe.cntExec++

	pe.chTasks <- ExecVersionView{ver: Version{tx, 0}, et: pe.tasks[tx], mvh: pe.mvh, sender: pe.tasks[tx].Sender()}

	return nil
}

func (pe *ParallelExecutor) Close(wait bool) {
	close(pe.chTasks)
	close(pe.chSpeculativeTasks)
	close(pe.chSettle)

	if wait {
		pe.workerWg.Wait()
	}

	if wait {
		pe.settleWg.Wait()
	}
}

// nolint: gocognit
func (pe *ParallelExecutor) Step(res *ExecResult) (result ParallelExecutionResult, err error) {
	tx := res.ver.TxnIndex

	if abortErr, ok := res.err.(ErrExecAbortError); ok && abortErr.OriginError != nil && pe.skipCheck[tx] {
		// If the transaction failed when we know it should not fail, this means the transaction itself is
		// bad (e.g. wrong nonce), and we should exit the execution immediately
		err = fmt.Errorf("could not apply tx %d [%v]: %w", tx, pe.tasks[tx].Hash(), abortErr.OriginError)
		pe.Close(true)

		return
	}

	// nolint: nestif
	if execErr, ok := res.err.(ErrExecAbortError); ok {
		addedDependencies := false

		if execErr.Dependency >= 0 {
			l := len(pe.estimateDeps[tx])
			for l > 0 && pe.estimateDeps[tx][l-1] > execErr.Dependency {
				pe.execTasks.removeDependency(pe.estimateDeps[tx][l-1])
				pe.estimateDeps[tx] = pe.estimateDeps[tx][:l-1]
				l--
			}

			addedDependencies = pe.execTasks.addDependencies(execErr.Dependency, tx)
		} else {
			estimate := 0

			if len(pe.estimateDeps[tx]) > 0 {
				estimate = pe.estimateDeps[tx][len(pe.estimateDeps[tx])-1]
			}

			addedDependencies = pe.execTasks.addDependencies(estimate, tx)

			newEstimate := estimate + (estimate+tx)/2
			if newEstimate >= tx {
				newEstimate = tx - 1
			}

			pe.estimateDeps[tx] = append(pe.estimateDeps[tx], newEstimate)
		}

		pe.execTasks.clearInProgress(tx)

		if !addedDependencies {
			pe.execTasks.pushPending(tx)
		}

		pe.txIncarnations[tx]++
		pe.diagExecAbort[tx]++
		pe.cntAbort++
	} else {
		pe.lastTxIO.recordRead(tx, res.txIn)

		if res.ver.Incarnation == 0 {
			pe.lastTxIO.recordWrite(tx, res.txOut)
			pe.lastTxIO.recordAllWrite(tx, res.txAllOut)
		} else {
			if res.txAllOut.hasNewWrite(pe.lastTxIO.AllWriteSet(tx)) {
				pe.validateTasks.pushPendingSet(pe.execTasks.getRevalidationRange(tx + 1))
			}

			prevWrite := pe.lastTxIO.AllWriteSet(tx)

			// Remove entries that were previously written but are no longer written

			cmpMap := make(map[Key]bool)

			for _, w := range res.txAllOut {
				cmpMap[w.Path] = true
			}

			for _, v := range prevWrite {
				if _, ok := cmpMap[v.Path]; !ok {
					pe.mvh.Delete(v.Path, tx)
				}
			}

			pe.lastTxIO.recordWrite(tx, res.txOut)
			pe.lastTxIO.recordAllWrite(tx, res.txAllOut)
		}

		pe.validateTasks.pushPending(tx)
		pe.execTasks.markComplete(tx)

		pe.diagExecSuccess[tx]++
		pe.cntSuccess++

		pe.execTasks.removeDependency(tx)
	}

	// do validations ...
	maxComplete := pe.execTasks.maxAllComplete()

	toValidate := make([]int, 0, 2)

	for pe.validateTasks.minPending() <= maxComplete && pe.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, pe.validateTasks.takeNextPending())
	}

	for i := 0; i < len(toValidate); i++ {
		pe.cntTotalValidations++

		tx := toValidate[i]

		if pe.skipCheck[tx] || ValidateVersion(tx, pe.lastTxIO, pe.mvh) {
			pe.validateTasks.markComplete(tx)
		} else {
			pe.cntValidationFail++

			pe.diagExecAbort[tx]++
			for _, v := range pe.lastTxIO.AllWriteSet(tx) {
				pe.mvh.MarkEstimate(v.Path, tx)
			}
			// 'create validation tasks for all transactions > tx ...'
			pe.validateTasks.pushPendingSet(pe.execTasks.getRevalidationRange(tx + 1))
			pe.validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes

			pe.execTasks.clearComplete(tx)
			pe.execTasks.pushPending(tx)

			pe.preValidated[tx] = false
			pe.txIncarnations[tx]++
		}
	}

	// Settle transactions that have been validated to be correct and that won't be re-executed again
	maxValidated := pe.validateTasks.maxAllComplete()

	for pe.lastSettled < maxValidated {
		pe.lastSettled++
		if pe.execTasks.checkInProgress(pe.lastSettled) || pe.execTasks.checkPending(pe.lastSettled) || pe.execTasks.isBlocked(pe.lastSettled) {
			pe.lastSettled--
			break
		}
		pe.chSettle <- pe.lastSettled
	}

	if pe.validateTasks.countComplete() == len(pe.tasks) && pe.execTasks.countComplete() == len(pe.tasks) {
		log.Debug("blockstm exec summary", "execs", pe.cntExec, "success", pe.cntSuccess, "aborts", pe.cntAbort, "validations", pe.cntTotalValidations, "failures", pe.cntValidationFail, "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(pe.tasks))/float64(pe.cntExec)*100))

		pe.Close(true)

		var allDeps map[int]map[int]bool

		var deps DAG

		if pe.profile {
			allDeps = GetDep(*pe.lastTxIO)
			deps = BuildDAG(*pe.lastTxIO)
		}

		return ParallelExecutionResult{pe.lastTxIO, pe.stats, &deps, allDeps}, err
	}

	// Send the next immediate pending transaction to be executed
	if pe.execTasks.minPending() != -1 && pe.execTasks.minPending() == maxValidated+1 {
		nextTx := pe.execTasks.takeNextPending()
		if nextTx != -1 {
			pe.cntExec++

			pe.skipCheck[nextTx] = true

			pe.chTasks <- ExecVersionView{ver: Version{nextTx, pe.txIncarnations[nextTx]}, et: pe.tasks[nextTx], mvh: pe.mvh, sender: pe.tasks[nextTx].Sender()}
		}
	}

	// Send speculative tasks
	for pe.execTasks.minPending() != -1 {
		nextTx := pe.execTasks.takeNextPending()

		if nextTx != -1 {
			pe.cntExec++

			task := ExecVersionView{ver: Version{nextTx, pe.txIncarnations[nextTx]}, et: pe.tasks[nextTx], mvh: pe.mvh, sender: pe.tasks[nextTx].Sender()}

			pe.specTaskQueue.Push(nextTx, task)
			pe.chSpeculativeTasks <- struct{}{}
		}
	}

	return
}