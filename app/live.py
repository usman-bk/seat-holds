import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict

from redis.asyncio import Redis
from redis.exceptions import RedisError

from .config import settings
from .holds import Change

log = logging.getLogger("seat_holds.live")
_CHANNEL_PREFIX = "seats:"


class Client:
    def __init__(self):
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=settings.client_queue_size)
        self.overflowed = asyncio.Event()

    def offer(self, message: dict) -> None:
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            # A client this far behind gets cut off and reconnects to a fresh
            # snapshot. Waiting for it would let one bad phone connection hold
            # up every other client in the process.
            self.overflowed.set()


class Hub:
    """One Redis subscription per process, fanned out to its WebSockets."""

    def __init__(self, redis: Redis):
        self.redis = redis
        self.clients: dict[int, set[Client]] = defaultdict(set)
        self._task: asyncio.Task | None = None
        self.subscribed = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._listen())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _listen(self) -> None:
        while True:
            pubsub = self.redis.pubsub()
            try:
                await pubsub.psubscribe(f"{_CHANNEL_PREFIX}*")
                self.subscribed.set()
                async for message in pubsub.listen():
                    if message["type"] == "pmessage":
                        self._dispatch(json.loads(message["data"]))
            except RedisError as exc:
                # Everything sent during the gap is lost, so whoever is
                # connected has a stale map. Drop them to force a resync.
                log.warning("redis subscription lost: %s", exc)
                self.subscribed.clear()
                self._drop_everyone()
                await asyncio.sleep(1)
            finally:
                await pubsub.aclose()

    def _dispatch(self, change: dict) -> None:
        for client in self.clients.get(change["event_id"], ()):
            client.offer(change)

    def _drop_everyone(self) -> None:
        for clients in self.clients.values():
            for client in clients:
                client.overflowed.set()

    def join(self, event_id: int) -> Client:
        client = Client()
        self.clients[event_id].add(client)
        return client

    def leave(self, event_id: int, client: Client) -> None:
        self.clients[event_id].discard(client)
        if not self.clients[event_id]:
            del self.clients[event_id]

    async def publish(self, changes: Iterable[Change]) -> None:
        # Postgres already holds the truth by the time this runs. If Redis is
        # down, the seat is still held and the maps catch up on reconnect.
        try:
            async with self.redis.pipeline(transaction=False) as pipe:
                for change in changes:
                    pipe.publish(f"{_CHANNEL_PREFIX}{change.event_id}", json.dumps(asdict(change)))
                await pipe.execute()
        except RedisError as exc:
            log.warning("publish failed: %s", exc)
