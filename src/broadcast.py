#!/usr/bin/env python3


from typing import Any, Dict, List, Set
from node import Node, Request, Body


messages: Set[int] = set()


async def read(req: Request) -> Body:
    return {"type": "read_ok", "messages": list(messages)}


async def broadcast(req: Request) -> Body:
    message = req.body["message"]
    messages.add(message)
    return {"type": "broadcast_ok"}


async def topology(req: Request) -> Body:
    # topology = req.body["topology"]
    return {"type": "topology_ok"}


if __name__ == "__main__":
    n = Node()
    n.on("read", read)
    n.on("broadcast", broadcast)
    n.on("topology", topology)
    n.run()
