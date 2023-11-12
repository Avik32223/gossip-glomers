#!/usr/bin/env python3
import ulid


from node import Node, Request, Body


class UniqueIdGeneratorNode(Node):
    async def generate(self, req: Request) -> Body:
        new_id = ulid.new()
        return {"type": "generate_ok", "id": new_id.str}


if __name__ == "__main__":
    n = UniqueIdGeneratorNode()
    n.on("generate", n.generate)
    n.run()
