#!/usr/bin/env python3

from node import Node, Request, Body


class EchoServer(Node):
    async def echo(self, req: Request) -> Body:
        return {"type": "echo_ok", "echo": req.body["echo"]}


if __name__ == "__main__":
    n = EchoServer()
    n.on("echo", n.echo)
    n.run()
