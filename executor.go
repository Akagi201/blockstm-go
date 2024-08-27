package blockstm

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

type ExecResult struct {
	err      error
	ver      Version
	txIn     TxnInput
	txOut    TxnOutput
	txAllOut TxnOutput
}

type ExecTask interface {
	Execute(mvh *MVHashMap, incarnation int) error
	MVReadList() []ReadDescriptor
	MVWriteList() []WriteDescriptor
	MVFullWriteList() []WriteDescriptor
	Hash() common.Hash
	Sender() common.Address
	Settle()
	Dependencies() []int
}

type ExecVersionView struct {
	ver    Version
	et     ExecTask
	mvh    *MVHashMap
	sender common.Address
}

func (ev *ExecVersionView) Execute() (er ExecResult) {
	er.ver = ev.ver
	if er.err = ev.et.Execute(ev.mvh, ev.ver.Incarnation); er.err != nil {
		return
	}

	er.txIn = ev.et.MVReadList()
	er.txOut = ev.et.MVWriteList()
	er.txAllOut = ev.et.MVFullWriteList()

	return
}

type ErrExecAbortError struct {
	Dependency  int
	OriginError error
}

func (e ErrExecAbortError) Error() string {
	if e.Dependency >= 0 {
		return fmt.Sprintf("Execution aborted due to dependency %d", e.Dependency)
	} else {
		return "Execution aborted"
	}
}

type ExecutionStat struct {
	TxIdx       int
	Incarnation int
	Start       uint64
	End         uint64
	Worker      int
}

type ParallelExecutor struct {
	// scheduler for task management
	scheduler *Scheduler

	// Number of workers that execute transactions speculatively
	numSpeculativeProcs int

	// A wait group to wait for all settling tasks to finish
	settleWg sync.WaitGroup

	// Channel to signal that a transaction has finished executing
	chResults chan struct{}

	// A priority queue that stores the transaction index of results, so we can validate the results in order
	resultQueue SafeQueue[ExecResult]

	// Channel to signal that the result of a transaction could be written to storage
	specTaskQueue SafeQueue[ExecVersionView]

	// Multi-version hash map
	mvh *MVHashMap

	// Time records when the parallel execution starts
	begin time.Time

	// Enable profiling
	profile bool

	// Worker wait group
	workerWg sync.WaitGroup
}

func NewParallelExecutor(tasks []ExecTask, profile bool, metadata bool, numProcs int) *ParallelExecutor {
	numTasks := len(tasks)
	var resultQueue SafeQueue[ExecResult]

	var specTaskQueue SafeQueue[ExecVersionView]

	if metadata {
		resultQueue = NewSafeFIFOQueue[ExecResult](numTasks)
		specTaskQueue = NewSafeFIFOQueue[ExecVersionView](numTasks)
	} else {
		resultQueue = NewSafePriorityQueue[ExecResult](numTasks)
		specTaskQueue = NewSafePriorityQueue[ExecVersionView](numTasks)
	}
	scheduler := NewScheduler(tasks)
	return &ParallelExecutor{
		scheduler:           scheduler,
		numSpeculativeProcs: numProcs,
		settleWg:            sync.WaitGroup{},
		chResults:           make(chan struct{}, numTasks),
		resultQueue:         resultQueue,
		specTaskQueue:       specTaskQueue,
		mvh:                 MakeMVHashMap(),
		begin:               time.Now(),
		profile:             profile,
	}
}

// nolint: gocognit
func (pe *ParallelExecutor) Prepare() error {
	prevSenderTx := make(map[common.Address]int)

	for i, t := range pe.scheduler.tasks {
		clearPendingFlag := false

		pe.scheduler.skipCheck[i] = false
		pe.scheduler.estimateDeps[i] = make([]int, 0)

		if len(t.Dependencies()) > 0 {
			for _, val := range t.Dependencies() {
				clearPendingFlag = true

				pe.scheduler.execTasks.addDependencies(val, i)
			}

			if clearPendingFlag {
				pe.scheduler.execTasks.clearPending(i)

				clearPendingFlag = false
			}
		} else {
			if tx, ok := prevSenderTx[t.Sender()]; ok {
				pe.scheduler.execTasks.addDependencies(tx, i)
				pe.scheduler.execTasks.clearPending(i)
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

				pe.resultQueue.Push(int64(res.ver.TxnIndex), res)
				pe.chResults <- struct{}{}

				if pe.profile {
					end := time.Since(pe.begin)
					pe.scheduler.stats.Insert(res.ver.TxnIndex, ExecutionStat{
						TxIdx:       res.ver.TxnIndex,
						Incarnation: res.ver.Incarnation,
						Start:       uint64(start),
						End:         uint64(end),
						Worker:      procNum,
					})
				}
			}

			if procNum < pe.numSpeculativeProcs {
				for range pe.scheduler.chSpeculativeTasks {
					doWork(pe.specTaskQueue.Pop())
				}
			} else {
				for task := range pe.scheduler.chTasks {
					doWork(task)
				}
			}
		}(i)
	}

	pe.settleWg.Add(1)

	go func() {
		for t := range pe.scheduler.chSettle {
			pe.scheduler.tasks[t].Settle()
		}

		pe.settleWg.Done()
	}()

	// bootstrap first execution
	tx := pe.scheduler.execTasks.takeNextPending()

	if tx == -1 {
		return ParallelExecFailedError{"no executable transactions due to bad dependency"}
	}

	pe.scheduler.cntExec++

	pe.scheduler.chTasks <- ExecVersionView{ver: Version{tx, 0}, et: pe.scheduler.tasks[tx], mvh: pe.mvh, sender: pe.scheduler.tasks[tx].Sender()}

	return nil
}

func (pe *ParallelExecutor) Close(wait bool) {
	close(pe.scheduler.chTasks)
	close(pe.scheduler.chSpeculativeTasks)
	close(pe.scheduler.chSettle)

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

	if abortErr, ok := res.err.(ErrExecAbortError); ok && abortErr.OriginError != nil && pe.scheduler.skipCheck[tx] {
		// If the transaction failed when we know it should not fail, this means the transaction itself is
		// bad (e.g. wrong nonce), and we should exit the execution immediately
		err = fmt.Errorf("could not apply tx %d [%v]: %w", tx, pe.scheduler.tasks[tx].Hash(), abortErr.OriginError)
		pe.Close(true)

		return
	}

	// nolint: nestif
	if execErr, ok := res.err.(ErrExecAbortError); ok {
		addedDependencies := false

		if execErr.Dependency >= 0 {
			l := len(pe.scheduler.estimateDeps[tx])
			for l > 0 && pe.scheduler.estimateDeps[tx][l-1] > execErr.Dependency {
				pe.scheduler.execTasks.removeDependency(pe.scheduler.estimateDeps[tx][l-1])
				pe.scheduler.estimateDeps[tx] = pe.scheduler.estimateDeps[tx][:l-1]
				l--
			}

			addedDependencies = pe.scheduler.execTasks.addDependencies(execErr.Dependency, tx)
		} else {
			estimate := 0

			if len(pe.scheduler.estimateDeps[tx]) > 0 {
				estimate = pe.scheduler.estimateDeps[tx][len(pe.scheduler.estimateDeps[tx])-1]
			}

			addedDependencies = pe.scheduler.execTasks.addDependencies(estimate, tx)

			newEstimate := estimate + (estimate+tx)/2
			if newEstimate >= tx {
				newEstimate = tx - 1
			}

			pe.scheduler.estimateDeps[tx] = append(pe.scheduler.estimateDeps[tx], newEstimate)
		}

		pe.scheduler.execTasks.clearInProgress(tx)

		if !addedDependencies {
			pe.scheduler.execTasks.pushPending(tx)
		}

		pe.scheduler.txIncarnations[tx]++
		pe.scheduler.diagExecAbort[tx]++
		pe.scheduler.cntAbort++
	} else {
		pe.scheduler.lastTxIO.recordRead(tx, res.txIn)

		if res.ver.Incarnation == 0 {
			pe.scheduler.lastTxIO.recordWrite(tx, res.txOut)
			pe.scheduler.lastTxIO.recordAllWrite(tx, res.txAllOut)
		} else {
			if res.txAllOut.hasNewWrite(pe.scheduler.lastTxIO.AllWriteSet(tx)) {
				pe.scheduler.validateTasks.pushPendingSet(pe.scheduler.execTasks.getRevalidationRange(tx + 1))
			}

			prevWrite := pe.scheduler.lastTxIO.AllWriteSet(tx)

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

			pe.scheduler.lastTxIO.recordWrite(tx, res.txOut)
			pe.scheduler.lastTxIO.recordAllWrite(tx, res.txAllOut)
		}

		pe.scheduler.validateTasks.pushPending(tx)
		pe.scheduler.execTasks.markComplete(tx)

		pe.scheduler.diagExecSuccess[tx]++
		pe.scheduler.cntSuccess++

		pe.scheduler.execTasks.removeDependency(tx)
	}

	// do validations ...
	maxComplete := pe.scheduler.execTasks.maxAllComplete()

	toValidate := make([]int, 0, 2)

	for pe.scheduler.validateTasks.minPending() <= maxComplete && pe.scheduler.validateTasks.minPending() >= 0 {
		toValidate = append(toValidate, pe.scheduler.validateTasks.takeNextPending())
	}

	for i := 0; i < len(toValidate); i++ {
		pe.scheduler.cntTotalValidations++

		tx := toValidate[i]

		if pe.scheduler.skipCheck[tx] || ValidateVersion(tx, pe.scheduler.lastTxIO, pe.mvh) {
			pe.scheduler.validateTasks.markComplete(tx)
		} else {
			pe.scheduler.cntValidationFail++

			pe.scheduler.diagExecAbort[tx]++
			for _, v := range pe.scheduler.lastTxIO.AllWriteSet(tx) {
				pe.mvh.MarkEstimate(v.Path, tx)
			}
			// 'create validation tasks for all transactions > tx ...'
			pe.scheduler.validateTasks.pushPendingSet(pe.scheduler.execTasks.getRevalidationRange(tx + 1))
			pe.scheduler.validateTasks.clearInProgress(tx) // clear in progress - pending will be added again once new incarnation executes

			pe.scheduler.execTasks.clearComplete(tx)
			pe.scheduler.execTasks.pushPending(tx)

			pe.scheduler.preValidated[tx] = false
			pe.scheduler.txIncarnations[tx]++
		}
	}

	// Settle transactions that have been validated to be correct and that won't be re-executed again
	maxValidated := pe.scheduler.validateTasks.maxAllComplete()

	for pe.scheduler.lastSettled < maxValidated {
		pe.scheduler.lastSettled++
		if pe.scheduler.execTasks.checkInProgress(pe.scheduler.lastSettled) || pe.scheduler.execTasks.checkPending(pe.scheduler.lastSettled) || pe.scheduler.execTasks.isBlocked(pe.scheduler.lastSettled) {
			pe.scheduler.lastSettled--
			break
		}
		pe.scheduler.chSettle <- pe.scheduler.lastSettled
	}

	if pe.scheduler.validateTasks.countComplete() == len(pe.scheduler.tasks) && pe.scheduler.execTasks.countComplete() == len(pe.scheduler.tasks) {
		log.Debug("blockstm exec summary", "execs", pe.scheduler.cntExec, "success", pe.scheduler.cntSuccess, "aborts", pe.scheduler.cntAbort, "validations", pe.scheduler.cntTotalValidations, "failures", pe.scheduler.cntValidationFail, "#tasks/#execs", fmt.Sprintf("%.2f%%", float64(len(pe.scheduler.tasks))/float64(pe.scheduler.cntExec)*100))

		pe.Close(true)

		var allDeps map[int]map[int]bool

		var deps DAG

		if pe.profile {
			allDeps = GetDep(*pe.scheduler.lastTxIO)
			deps = BuildDAG(*pe.scheduler.lastTxIO)
		}

		return ParallelExecutionResult{pe.scheduler.lastTxIO, pe.scheduler.stats, &deps, allDeps}, err
	}

	// Send the next immediate pending transaction to be executed
	if pe.scheduler.execTasks.minPending() != -1 && pe.scheduler.execTasks.minPending() == maxValidated+1 {
		nextTx := pe.scheduler.execTasks.takeNextPending()
		if nextTx != -1 {
			pe.scheduler.cntExec++

			pe.scheduler.skipCheck[nextTx] = true

			pe.scheduler.chTasks <- ExecVersionView{ver: Version{nextTx, pe.scheduler.txIncarnations[nextTx]}, et: pe.scheduler.tasks[nextTx], mvh: pe.mvh, sender: pe.scheduler.tasks[nextTx].Sender()}
		}
	}

	// Send speculative tasks
	for pe.scheduler.execTasks.minPending() != -1 {
		nextTx := pe.scheduler.execTasks.takeNextPending()

		if nextTx != -1 {
			pe.scheduler.cntExec++

			task := ExecVersionView{ver: Version{nextTx, pe.scheduler.txIncarnations[nextTx]}, et: pe.scheduler.tasks[nextTx], mvh: pe.mvh, sender: pe.scheduler.tasks[nextTx].Sender()}

			pe.specTaskQueue.Push(int64(nextTx), task)
			pe.scheduler.chSpeculativeTasks <- struct{}{}
		}
	}

	return
}
