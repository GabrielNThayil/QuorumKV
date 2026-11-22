"""Peer-to-peer message transport over TCP with length-prefixed JSON frames.

One long-lived outbound connection per peer, with reconnect and backoff. Messages to
an unreachable peer are dropped rather than queued: Raft retransmits on its own
schedule, so stale backlogs would only waste bandwidth after a partition heals.
"""
from __future__ import annotations

import asyncio
import json
import struct
from typing import Callable

_LEN = struct.Struct(">I")
MAX_FRAME = 64 << 20


def encode_frame(obj) -> bytes:
    data = json.dumps(obj, separators=(",", ":")).encode()
    return _LEN.pack(len(data)) + data


async def read_frame(reader: asyncio.StreamReader):
    (n,) = _LEN.unpack(await reader.readexactly(_LEN.size))
    if n > MAX_FRAME:
        raise ValueError("frame too large")
    return json.loads(await reader.readexactly(n))


class PeerTransport:
    def __init__(self, node_id: str, peers: dict[str, tuple[str, int]], on_message: Callable[[dict], None]):
        self.node_id = node_id
        self.peers = peers
        self.on_message = on_message
        self._queues: dict[str, asyncio.Queue] = {}
        self._connected: dict[str, bool] = {p: False for p in peers}
        self._tasks: list[asyncio.Task] = []
        self._server: asyncio.base_events.Server | None = None
        self.dropped = 0
        self.sent = 0

    async def start(self, host: str, port: int) -> None:
        self._server = await asyncio.start_server(self._serve, host, port)
        for p in self.peers:
            self._queues[p] = asyncio.Queue(maxsize=4096)
            self._tasks.append(asyncio.create_task(self._sender(p)))

    def send(self, msg: dict) -> None:
        to = msg["to"]
        q = self._queues.get(to)
        if q is None or not self._connected[to]:
            self.dropped += 1
            return
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            self.dropped += 1

    async def _sender(self, peer: str) -> None:
        host, port = self.peers[peer]
        q = self._queues[peer]
        backoff = 0.05
        while True:
            try:
                _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=1.0)
            except (OSError, asyncio.TimeoutError):
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 1.0)
                continue
            backoff = 0.05
            self._connected[peer] = True
            try:
                while True:
                    msg = await q.get()
                    batch = [encode_frame(msg)]
                    while not q.empty() and len(batch) < 256:
                        batch.append(encode_frame(q.get_nowait()))
                    writer.write(b"".join(batch))
                    await writer.drain()
                    self.sent += len(batch)
            except (OSError, ConnectionError):
                pass
            finally:
                self._connected[peer] = False
                while not q.empty():
                    q.get_nowait()
                    self.dropped += 1
                writer.close()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                self.on_message(await read_frame(reader))
        except (asyncio.IncompleteReadError, OSError, ValueError):
            pass
        finally:
            writer.close()

    async def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._server:
            self._server.close()
