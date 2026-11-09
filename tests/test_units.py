import asyncio
import time

from app import auth
from app.holds import Change
from app.live import Client, Hub


class BrokenRedis:
    def pipeline(self, transaction=False):
        from redis.exceptions import ConnectionError as RedisDown

        raise RedisDown("gone")


async def test_slow_client_is_marked_and_does_not_block():
    client = Client()
    capacity = client.queue.maxsize

    for n in range(capacity + 5):
        client.offer({"n": n})

    assert client.overflowed.is_set()
    assert client.queue.qsize() == capacity


async def test_publish_survives_redis_being_down():
    await Hub(BrokenRedis()).publish([Change(1, 1, 1, "held")])


async def test_dispatch_goes_only_to_that_event():
    hub = Hub(BrokenRedis())
    here, there = hub.join(1), hub.join(2)

    hub._dispatch({"event_id": 1, "seat_id": 9})

    assert here.queue.qsize() == 1
    assert there.queue.qsize() == 0


async def test_last_client_leaving_clears_the_event():
    hub = Hub(BrokenRedis())
    client = hub.join(1)

    hub.leave(1, client)

    assert 1 not in hub.clients
    await asyncio.sleep(0)


class SlowHasher:
    def verify(self, stored, password):
        time.sleep(0.4)

    def hash(self, password):
        time.sleep(0.4)


async def _longest_gap_while(work) -> float:
    gaps = []

    async def ticker():
        while True:
            before = time.monotonic()
            await asyncio.sleep(0.005)
            gaps.append(time.monotonic() - before)

    running = asyncio.create_task(ticker())
    await asyncio.sleep(0.02)
    await work
    await asyncio.sleep(0.02)  # let the ticker report the gap it was stuck in
    running.cancel()
    return max(gaps)


async def test_verifying_a_password_leaves_the_loop_free(monkeypatch):
    monkeypatch.setattr(auth, "_hasher", SlowHasher())

    assert await _longest_gap_while(auth.verify_password("stored", "pw")) < 0.2


async def test_hashing_a_password_leaves_the_loop_free(monkeypatch):
    monkeypatch.setattr(auth, "_hasher", SlowHasher())

    assert await _longest_gap_while(auth.hash_password("pw")) < 0.2
