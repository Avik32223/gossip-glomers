#!/usr/bin/env python3

from node import Node, Request, Body


async def echo(node: Node, req: Request) -> Body:
    return {"type": "echo_ok", "echo": req.body["echo"]}


if __name__ == "__main__":
    n = Node()
    n.on("echo", echo)
    n.run()
