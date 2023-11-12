#!/usr/bin/env python3


import asyncio
from typing import Any, Awaitable, Dict, Set
from node import Node, Request, Body


messages: Set[int] = set()
neighbour_broadcasts: Set[asyncio.Task] = set()


async def read(node: Node, req: Request) -> Body:
    return {"type": "read_ok", "messages": list(messages)}


async def broadcast(node: Node, req: Request) -> Body:
    message = req.body["message"]
    messages.add(message)
    return {"type": "broadcast_ok"}


async def broadcast_to_neighbour(node: Node, neighbour_id):
    sent = set()
    while True:
        diff = list(messages - sent)
        if diff:
            message = diff[0]
            await node.send_request(
                Request(
                    node.node_id,
                    neighbour_id,
                    {"type": "broadcast", "message": message},
                )
            )
            sent.add(message)
        else:
            await asyncio.sleep(1)


async def topology(node: Node, req: Request) -> Body:
    topology = req.body["topology"]
    neighbours = topology[node.node_id]
    for n in neighbour_broadcasts:
        n.cancel()

    for n in neighbours:
        task = asyncio.create_task(broadcast_to_neighbour(node, n))
        neighbour_broadcasts.add(task)
        task.add_done_callback(neighbour_broadcasts.discard)

    return {"type": "topology_ok"}


if __name__ == "__main__":
    n = Node()
    n.on("read", read)
    n.on("broadcast", broadcast)
    n.on("topology", topology)
    n.run()
