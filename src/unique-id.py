#!/usr/bin/env python3
import ulid


from node import Node, Request, Body


async def generate(node: Node, req: Request) -> Body:
    new_id = ulid.new()
    return {"type": "generate_ok", "id": new_id.str}


if __name__ == "__main__":
    n = Node()
    n.on("generate", generate)
    n.run()
