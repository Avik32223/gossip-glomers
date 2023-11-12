#!/usr/bin/env python3


import asyncio
import json
import sys
from typing import Any, Awaitable, Dict, Set
from node import Node, Request, Body


messages: Set[int] = set()
new_messages = asyncio.Condition()
neighbour_broadcasts: Set[asyncio.Task] = set()


async def read(node: Node, req: Request) -> Body:
    return {"type": "read_ok", "messages": list(messages)}


async def broadcast(node: Node, req: Request) -> Body:
    message = req.body["message"]
    _existing = len(messages)
    messages.add(message)
    if _existing != len(messages):
        async with new_messages:
            new_messages.notify_all()
    return {"type": "broadcast_ok"}


async def broadcast_many(node: Node, req: Request) -> Body:
    _messages = req.body["messages"]
    _existing = len(messages)
    messages.update(_messages)
    if _existing != len(messages):
        async with new_messages:
            new_messages.notify_all()

    return {"type": "broadcast_many_ok"}


async def broadcast_to_neighbour(node: Node, neighbour_id):
    sent = set()
    while True:
        diff = list(messages - sent)
        if diff:
            resp = await node.send_request_and_wait_for_response(
                Request(
                    node.node_id,
                    neighbour_id,
                    {"type": "broadcast_many", "messages": diff},
                )
            )
            await node.log(json.dumps(resp))
            if resp["type"] == "broadcast_many_ok":
                sent.update(diff)
        else:
            async with new_messages:
                await new_messages.wait_for(lambda: len(sent) != len(messages))


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
    n.on("broadcast_many", broadcast_many)
    n.on("topology", topology)
    n.run()
