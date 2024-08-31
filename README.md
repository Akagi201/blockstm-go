# blockstm-go

General Block-STM execution engine for EVM in Go

## Features

- Block-STM with lazy update.
- Minimize state synchronization overheads.
- Optimized concurrent data structures.
- Support optional DAG for full nodes to sync from sequencer faster.
- Support any executor.

## Libraries

- Use [hashmap](https://github.com/cornelk/hashmap) for type-safe lock-free thread-safe concurrent map.
- Use [gpq](https://github.com/JustinTimperio/gpq) for concurrent safe priority queue.

## TODOs

- Use atomic index to manage validation and execution tasks.
- Implement an EVM executor task.
- Test real mainnet blocks, and validate the results.
- Benchmarks between parallel and sequential execution.
- Profiling and optimizations.
