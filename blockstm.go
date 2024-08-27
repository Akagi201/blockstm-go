package blockstm

import "context"

type PropertyCheck func(*ParallelExecutor) error

func executeParallelWithCheck(tasks []ExecTask, profile bool, check PropertyCheck, metadata bool, numProcs int, interruptCtx context.Context) (result ParallelExecutionResult, err error) {
	if len(tasks) == 0 {
		return ParallelExecutionResult{MakeTxnInputOutput(len(tasks)), nil, nil, nil}, nil
	}

	pe := NewParallelExecutor(tasks, profile, metadata, numProcs)
	err = pe.Prepare()

	if err != nil {
		pe.Close(true)
		return
	}

	for range pe.chResults {
		if interruptCtx != nil && interruptCtx.Err() != nil {
			pe.Close(true)
			return result, interruptCtx.Err()
		}

		res := pe.resultQueue.Pop()

		result, err = pe.Step(&res)

		if err != nil {
			return result, err
		}

		if check != nil {
			err = check(pe)
		}

		if result.TxIO != nil || err != nil {
			return result, err
		}
	}

	return
}

func ExecuteParallel(tasks []ExecTask, profile bool, metadata bool, numProcs int, interruptCtx context.Context) (result ParallelExecutionResult, err error) {
	return executeParallelWithCheck(tasks, profile, nil, metadata, numProcs, interruptCtx)
}
