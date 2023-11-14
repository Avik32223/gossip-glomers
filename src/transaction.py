#!/usr/bin/env python3

from typing import Dict
from node import Node, Request, Body

state: Dict[int, int] = dict()


async def transaction(node: Node, req: Request) -> Body:
    txn = req.body["txn"]
    result = list()
    for op in txn:
        op_type, key, value = op
        if op_type == "w":
            state[key] = value
        result.append((op_type, key, state.get(key)))

    return {"type": "txn_ok", "txn": result}


if __name__ == "__main__":
    n = Node()
    n.on("txn", transaction)
    n.run()
