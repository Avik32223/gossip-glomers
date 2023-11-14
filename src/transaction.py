#!/usr/bin/env python3

from asyncio import sleep
from typing import Dict, List, Tuple
from node import Node, Request, Body

state: Dict[int, int] = dict()


async def transaction(node: Node, req: Request) -> Body:
    txn = req.body["txn"]
    result = list()
    for op in txn:
        op_type, key, value = op
        server_idx = int(key) % len(node.node_ids)
        server_id = node.node_ids[server_idx]
        if server_id == node.node_id:
            if op_type == "w":
                state[key] = value
            result.append((op_type, key, state.get(key)))
        else:
            req_body = {"type": "txn", "txn": [op]}
            server_req = Request(node.node_id, server_id, req_body)
            resp = await node.send_request_and_wait_for_response(server_req)
            result.append(resp["txn"][0])

    return {"type": "txn_ok", "txn": result}


async def merge(node: Node, req: Request) -> Body:
    updated_state = req.body["state"]
    state.update(updated_state)

    return {"type": "merge_ok"}


async def synchronize(node: Node, node_id: str):
    while True:
        req = Request(node.node_id, node_id, {"type": "merge", "state": state})
        await node.send_request_and_wait_for_response(req)
        await sleep(1)


async def start_replication(node: Node, req: Request):
    for node_id in node.node_ids:
        if node_id != node.node_id:
            await node.split_and_run(synchronize(node, node_id))


if __name__ == "__main__":
    n = Node()
    n.on("txn", transaction)
    n.on("merge", merge)
    n.run(start_replication)
