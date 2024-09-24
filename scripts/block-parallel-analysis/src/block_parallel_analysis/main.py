import os

import requests


def show_progress(prefix, iteration, total):
    progress = (iteration / total) * 100
    bar_length = 20
    filled_length = int(bar_length * progress // 100)
    bar = "=" * filled_length + "-" * (bar_length - filled_length)
    print(
        f"\rProgress {prefix}: [{bar}] {progress:.2f}% Complete ({iteration}/{total})",
        end="",
        flush=True,
    )


accounts2ignore = set(
    [
        "0x4200000000000000000000000000000000000019",  # base fee vault
        "0x420000000000000000000000000000000000001a",  # l1 fee vault
        "0x4200000000000000000000000000000000000015",
        "0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001",
        "0x0000000000000000000000000000000000000004",
        "0x4200000000000000000000000000000000000011",  # seq fee vault
        "0xa4b000000000000000000073657175656e636572",  # block builder for arb
        "0x0000000000000000000000000000000000000001",  # special arb address
    ]
)


def get_block_transactions(block_number, rpc_url):
    # Construct JSON-RPC request
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block_number), True],  # True for including transactions
        "id": 1,
    }

    # Make the request
    response = requests.post(rpc_url, json=payload)
    data = response.json()

    # Extract transaction hashes from the block data
    transactions = data["result"]["transactions"][1:]

    transaction_hashes = [tx["hash"] for tx in transactions]

    return transaction_hashes


def trace_read_set(tx_hash, rpc_url):
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        "params": [tx_hash, {"tracer": "prestateTracer", "diffMode": False}],
        "id": 1,
    }

    response = requests.post(rpc_url, json=payload)
    data = response.json()

    accounts = set(data["result"].keys()) - accounts2ignore
    storage_set = set()
    for acc in accounts:
        if "storage" not in data["result"][acc]:
            storage_set.add(acc)
            continue
        storage_keys = data["result"][acc]["storage"].keys()
        storage_set.update([acc + key for key in storage_keys])

    return storage_set


def trace_write_set(tx_hash, rpc_url):
    payload = {
        "jsonrpc": "2.0",
        "method": "debug_traceTransaction",
        "params": [tx_hash, {"tracer": "prestateTracer", "diffMode": True}],
        "id": 1,
    }

    response = requests.post(rpc_url, json=payload)
    data = response.json()
    accounts = set(data["result"].keys()) - accounts2ignore

    storage_set = set()
    for acc in accounts:
        if "storage" not in data["result"][acc]:
            storage_set.add(acc)
            continue
        storage_keys = data["result"][acc]["storage"].keys()
        storage_set.update([acc + key for key in storage_keys])

    return storage_set


def main():
    rpc_url = os.environ.get("OPTIMISM_RPC_URL")
    start_block = 125640099
    end_block = start_block + 2
    print(
        f"Block parallel analysis, start block: {start_block}, end block: {end_block}, total blocks: {end_block-start_block}"
    )
    blocks = []
    dep_tx_pairs_count = []
    for block in range(start_block, end_block):
        txs_read_write_sets = []

        blocks.append(block - start_block)
        txs = get_block_transactions(block, rpc_url)
        dep_tx_pairs = []
        dep_txs = set()
        for index, tx in enumerate(txs):
            show_progress(
                f"Tracing txs in block ({block-start_block}/{end_block-start_block})",
                index,
                len(txs),
            )
            txs_read_write_sets.append(
                {
                    "tx": tx,
                    "read_set": trace_read_set(tx, rpc_url),
                    "write_set": trace_write_set(tx, rpc_url),
                }
            )
        for index, tx_set in enumerate(txs_read_write_sets):
            for i in range(index):
                if tx_set["read_set"] & txs_read_write_sets[i]["write_set"]:
                    # intersection_set = tx_set["read_set"].intersection(
                    #     txs_read_write_sets[i]["write_set"]
                    # )
                    # print((i, index), intersection_set)
                    dep_tx_pairs.append((i, index))
                    dep_txs.add(index)
        dep_tx_pairs_count.append(len(dep_tx_pairs))
        print(
            f"\nBlock {block} has {len(dep_tx_pairs)} dep tx pairs, dep txs: {len(dep_txs)} total txs: {len(txs)}"
        )
        print(f"dep tx pairs: {dep_tx_pairs}")
    print(dep_tx_pairs_count)


if __name__ == "__main__":
    main()
