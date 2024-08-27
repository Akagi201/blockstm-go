package blockstm

import (
	"github.com/cornelk/hashmap"
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

type Scheduler struct {
	tasks []ExecTask

	// Stores the execution statistics for the last incarnation of each task
	stats *hashmap.Map[int, ExecutionStat]

	// Channel for tasks that should be prioritized
	chTasks chan ExecVersionView

	// Channel for speculative tasks
	chSpeculativeTasks chan struct{}

	// A priority queue that stores speculative tasks
	chSettle chan int

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

	// Stores the inputs and outputs of the last incardanotion of all transactions
	lastTxIO *TxnInputOutput

	// Tracks the incarnation number of each transaction
	txIncarnations []int

	// A map that stores the estimated dependency of a transaction if it is aborted without any known dependency
	estimateDeps map[int][]int

	// A map that records whether a transaction result has been speculatively validated
	preValidated map[int]bool
}

func NewScheduler(tasks []ExecTask) *Scheduler {
	numTasks := len(tasks)
	s := &Scheduler{
		tasks:              tasks,
		stats:              hashmap.NewSized[int, ExecutionStat](uintptr(numTasks)),
		chTasks:            make(chan ExecVersionView, numTasks),
		chSpeculativeTasks: make(chan struct{}, numTasks),
		chSettle:           make(chan int, numTasks),
		lastSettled:        -1,
		skipCheck:          make(map[int]bool),
		execTasks:          makeStatusManager(numTasks),
		validateTasks:      makeStatusManager(0),
		diagExecSuccess:    make([]int, numTasks),
		diagExecAbort:      make([]int, numTasks),
		lastTxIO:           MakeTxnInputOutput(numTasks),
		txIncarnations:     make([]int, numTasks),
		estimateDeps:       make(map[int][]int),
		preValidated:       make(map[int]bool),
	}

	return s
}
