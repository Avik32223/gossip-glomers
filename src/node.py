import asyncio
from copy import deepcopy
from asyncio.locks import Lock
from typing import Any, Awaitable, Callable, Dict, List, Set, TypeAlias
from dataclasses import dataclass
from sys import stdin, stderr
import json

Body: TypeAlias = Dict[str, Any]


@dataclass
class Request:
    src: str
    dest: str
    body: Body


class Node:
    node_id: str
    node_ids: List[str]
    next_msg_id: int
    stdin_lock: Lock
    stdout_lock: Lock
    stderr_lock: Lock
    _handlers: Dict[str, Callable[["Node", Request], Awaitable[Body]]]
    _tasks: Set[asyncio.Task]
    _futures: Dict[int, asyncio.Future]

    def __init__(self, node_id="") -> None:
        self.node_id = node_id
        self.next_msg_id = 1
        self.stdin_lock = Lock()
        self.stdout_lock = Lock()
        self.stderr_lock = Lock()
        self._handlers = dict()
        self._tasks = set()
        self._futures = dict()

    async def log(self, msg):
        loop = asyncio.get_event_loop()
        async with self.stderr_lock:
            await loop.run_in_executor(
                None, lambda: print(msg, flush=True, file=stderr)
            )

    async def send_request_and_wait_for_response(self, req: Request) -> Body:
        loop = asyncio.get_event_loop()
        msg_id = self.next_msg_id
        req.body["msg_id"] = msg_id
        self.next_msg_id += 1
        fut = loop.create_future()
        self._futures[msg_id] = fut

        await self.send_request(req)
        try:
            async with asyncio.timeout(1):
                return await fut
        except asyncio.TimeoutError:
            return {
                "type": "error",
                "in_reply_to": msg_id,
                "code": "0",  # Error type: timeout
                "text": "TIMEOUT",
            }
        finally:
            self._futures.pop(msg_id, None)

    async def send_request(self, req: Request) -> None:
        loop = asyncio.get_event_loop()
        data = json.dumps({"src": req.src, "dest": req.dest, "body": req.body})
        async with self.stdout_lock:
            await loop.run_in_executor(None, lambda: print(data, flush=True))

    async def receive_request(self) -> Request:
        loop = asyncio.get_event_loop()
        async with self.stdin_lock:
            line = await loop.run_in_executor(None, stdin.readline)
            req = json.loads(line)
            return Request(req["src"], req["dest"], req["body"])

    async def _handle_request(self, req: Request):
        req_type = req.body["type"]
        if req_type in self._handlers:
            handler = self._handlers[req_type]
            reply_body = await handler(self, deepcopy(req))
        else:
            reply_body = {
                "type": "error",
                "code": "10",  # Error type: not supported
                "text": "RPC type is not supported",
            }
        reply_body["in_reply_to"] = req.body["msg_id"]
        await self.send_request(Request(self.node_id, req.src, reply_body))

    async def _run(self):
        req = await self.receive_request()
        if req.body["type"] != "init":
            raise Exception("Init message not received yet")
        self.node_id = req.body["node_id"]
        self.node_ids = req.body["node_ids"]
        resp_body = {"type": "init_ok", "in_reply_to": req.body["msg_id"]}
        await self.send_request(Request(self.node_id, req.src, resp_body))

        while True:
            req = await self.receive_request()
            if req.body.get("in_reply_to"):
                reply_id = req.body["in_reply_to"]
                fut = self._futures.pop(reply_id, None)
                if fut is not None:
                    try:
                        fut.set_result(req.body)
                    except asyncio.InvalidStateError:
                        pass
                continue
            task = asyncio.create_task(self._handle_request(req))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def run(self, *args, **kwargs):
        asyncio.run(self._run(*args, **kwargs))

    def on(self, type: str, handler: Callable[["Node", Request], Awaitable]):
        self._handlers[type] = handler
