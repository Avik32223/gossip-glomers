#!/usr/bin/env python3

from collections import defaultdict
from typing import Dict, List, Tuple
from node import Node, Request, Body

offset = 0
klog: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
kcommits: Dict[str, int] = dict()


async def send(node: Node, req: Request) -> Body:
    global offset
    offset += 1
    key_offset = offset
    key = req.body["key"]
    msg = req.body["msg"]
    klog[key].append(tuple([key_offset, msg]))
    return {"type": "send_ok", "offset": key_offset}


async def poll(node: Node, req: Request) -> Body:
    result = defaultdict(list)
    for key, req_offset in req.body["offsets"].items():
        entries = klog[key]
        for koffset, kmsg in entries:
            if koffset >= req_offset:
                result[key].append(tuple([koffset, kmsg]))
            if len(result[key]) > 3:
                break

    return {"type": "poll_ok", "msgs": result}


async def commit_offsets(node: Node, req: Request) -> Body:
    offsets = req.body["offsets"]
    kcommits.update(offsets)
    return {"type": "commit_offsets_ok"}


async def list_committed_offsets(node: Node, req: Request) -> Body:
    result = {}
    keys = req.body["keys"]
    for key in keys:
        if key in kcommits:
            result[key] = kcommits[key]

    return {"type": "list_committed_offsets_ok", "offsets": result}


if __name__ == "__main__":
    n = Node()
    n.on("send", send)
    n.on("poll", poll)
    n.on("commit_offsets", commit_offsets)
    n.on("list_committed_offsets", list_committed_offsets)
    n.run()
