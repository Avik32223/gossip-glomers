#!/usr/bin/env python3

from asyncio import sleep
from collections import defaultdict
from typing import Dict
from node import Node, Request, Body

state: Dict[str, int] = defaultdict(int)


async def add(node: Node, req: Request) -> Body:
    delta = req.body["delta"]
    state[node.node_id] += delta
    return {"type": "add_ok"}


async def read(node: Node, req: Request) -> Body:
    total = 0
    for v in state.values():
        total += v
    return {"type": "read_ok", "value": total}


async def merge(node: Node, req: Request) -> Body:
    merge_state = req.body["state"]
    for i, v in merge_state.items():
        state[i] = max(state[i], v)

    return {"type": "merge_ok", "value": state}


async def synchronize(node: Node, node_id: str):
    while True:
        req = Request(node.node_id, node_id, {"type": "merge", "state": state})
        await node.send_request(req)
        await sleep(2)


async def start_replication(node: Node, req: Request):
    for node_id in node.node_ids:
        if node_id != node.node_id:
            await node.split_and_run(synchronize(node, node_id))


if __name__ == "__main__":
    n = Node()
    n.on("add", add)
    n.on("read", read)
    n.on("merge", merge)
    n.run(start_replication)
