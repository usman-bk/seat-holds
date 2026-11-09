import asyncio
import json

import websockets

from tests.conftest import expire, place


def ws_url(base: str, event_id: int) -> str:
    return base.replace("http://", "ws://") + f"/events/{event_id}/live"


async def next_change(socket) -> dict:
    return json.loads(await asyncio.wait_for(socket.recv(), 5))


async def snapshot_of(socket) -> dict:
    message = await next_change(socket)
    assert message["type"] == "snapshot"
    return {s["id"]: s for s in message["seats"]}


async def test_snapshot_on_connect(servers, http, user, event):
    event_id, seats = event
    await place(http, user, event_id, seats[:1])

    async with websockets.connect(ws_url(servers.a, event_id)) as socket:
        state = await snapshot_of(socket)

    assert state[seats[0]]["state"] == "held"
    assert state[seats[1]]["state"] == "free"


async def test_change_made_through_the_other_process(servers, http, user, event):
    event_id, seats = event

    async with websockets.connect(ws_url(servers.b, event_id)) as socket:
        await snapshot_of(socket)
        await place(http, user, event_id, seats[:1])  # http goes to server A

        change = await next_change(socket)

    assert (change["seat_id"], change["state"]) == (seats[0], "held")


async def test_held_sold_free_each_arrive_in_order(servers, http, user, event):
    event_id, seats = event

    async with websockets.connect(ws_url(servers.a, event_id)) as socket:
        await snapshot_of(socket)
        hold_id, _ = await place(http, user, event_id, seats[:1])
        await http.delete(f"/holds/{hold_id}", headers=user)
        second, _ = await place(http, user, event_id, seats[:1])
        await http.post(f"/holds/{second}/confirm", headers=user)

        changes = [await next_change(socket) for _ in range(4)]

    assert [c["state"] for c in changes] == ["held", "free", "held", "sold"]
    assert [c["version"] for c in changes] == sorted(c["version"] for c in changes)


async def test_other_events_are_not_delivered(servers, http, user, event, engine):
    event_id, seats = event

    async with websockets.connect(ws_url(servers.a, event_id + 100000)) as socket:
        try:
            await asyncio.wait_for(socket.recv(), 3)
        except websockets.ConnectionClosed as exc:
            assert exc.rcvd.code == 4404
        else:
            raise AssertionError("an unknown event should be closed")


async def test_expired_hold_is_announced_by_the_sweeper(servers, http, user, event, engine):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])

    async with websockets.connect(ws_url(servers.a, event_id)) as socket:
        await snapshot_of(socket)
        await expire(engine, hold_id)

        change = await next_change(socket)

    assert (change["seat_id"], change["state"]) == (seats[0], "free")


async def test_two_processes_sweep_without_repeating(servers, http, user, event, engine):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:6])

    async with websockets.connect(ws_url(servers.a, event_id)) as socket:
        await snapshot_of(socket)
        await expire(engine, hold_id)
        changes = [await next_change(socket) for _ in range(6)]
        extra = None
        try:
            extra = await asyncio.wait_for(socket.recv(), 1)
        except TimeoutError:
            pass

    assert sorted(c["seat_id"] for c in changes) == sorted(seats[:6])
    assert extra is None


async def test_many_viewers_do_not_use_up_the_pool(servers, http, user, event):
    """Pool is 10 connections. Sixty sockets stay open while requests still work."""
    event_id, seats = event
    sockets = await asyncio.gather(
        *(websockets.connect(ws_url(servers.a, event_id)) for _ in range(60))
    )
    try:
        for socket in sockets:
            await snapshot_of(socket)

        _, response = await asyncio.wait_for(place(http, user, event_id, seats[:1]), 5)
        assert response.status_code == 201

        changes = await asyncio.gather(*(next_change(socket) for socket in sockets))
        assert all(c["seat_id"] == seats[0] for c in changes)
    finally:
        await asyncio.gather(*(socket.close() for socket in sockets))
