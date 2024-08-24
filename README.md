# blockstm-go

General Block-STM execution engine for EVM in Go

## Features

- Use [hashmap](https://github.com/cornelk/hashmap) for type-safe lock-free thread-safe concurrent map.
- Use [gpq](https://github.com/JustinTimperio/gpq) for concurrent safe priority queue.
- Use atomic index to manage validation and execution tasks.
