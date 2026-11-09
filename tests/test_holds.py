import asyncio
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.conftest import expire, make_user, place, read_seat


async def test_hold_then_confirm(http, user, event, engine):
    event_id, seats = event

    hold_id, response = await place(http, user, event_id, seats[:2])
    assert response.status_code == 201

    assert (await http.post(f"/holds/{hold_id}/confirm", headers=user)).status_code == 200
    assert (await read_seat(engine, seats[0])).sold_to is not None


async def test_held_seat_refused_for_someone_else(http, user, other, event):
    event_id, seats = event
    await place(http, user, event_id, seats[:1])

    _, response = await place(http, other, event_id, seats[:1])

    assert response.status_code == 409
    assert response.json()["seat_ids"] == seats[:1]


async def test_group_is_all_or_nothing(http, user, other, event, engine):
    event_id, seats = event
    await place(http, user, event_id, [seats[2]])

    _, response = await place(http, other, event_id, seats[:4])

    assert response.status_code == 409
    assert response.json()["seat_ids"] == [seats[2]]
    assert (await read_seat(engine, seats[0])).hold_id is None


async def test_same_put_twice_is_the_same_hold(http, user, event):
    event_id, seats = event

    hold_id, first = await place(http, user, event_id, seats[:3])
    _, second = await place(http, user, event_id, seats[:3], hold_id)

    assert second.status_code == 201
    assert second.json()["expires_at"] == first.json()["expires_at"]


async def test_someone_elses_hold_id_gets_nothing(http, user, other, event):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])

    _, response = await place(http, other, event_id, seats[:1], hold_id)

    assert response.status_code == 409


async def test_expired_hold_is_available_without_the_sweeper(http, user, other, event, engine):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])
    await expire(engine, hold_id)

    _, response = await place(http, other, event_id, seats[:1])

    assert response.status_code == 201


async def test_confirm_after_expiry_is_gone(http, user, event, engine):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])
    await expire(engine, hold_id)

    assert (await http.post(f"/holds/{hold_id}/confirm", headers=user)).status_code == 410


async def test_confirm_twice(http, user, event):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])

    first = await http.post(f"/holds/{hold_id}/confirm", headers=user)
    second = await http.post(f"/holds/{hold_id}/confirm", headers=user)

    assert (first.status_code, second.status_code) == (200, 200)


async def test_only_the_holder_can_confirm(http, user, other, event):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])

    assert (await http.post(f"/holds/{hold_id}/confirm", headers=other)).status_code == 410


async def test_sold_seat_stays_sold(http, user, other, event):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])
    await http.post(f"/holds/{hold_id}/confirm", headers=user)

    _, response = await place(http, other, event_id, seats[:1])

    assert response.status_code == 409


async def test_release(http, user, other, event):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])

    assert (await http.delete(f"/holds/{hold_id}", headers=user)).status_code == 204

    _, response = await place(http, other, event_id, seats[:1])
    assert response.status_code == 201


async def test_cannot_release_another_persons_hold(http, user, other, event, engine):
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])

    await http.delete(f"/holds/{hold_id}", headers=other)

    assert (await read_seat(engine, seats[0])).hold_id == hold_id


async def test_seat_from_another_event(http, user, event, engine):
    event_id, seats = event

    _, response = await place(http, user, event_id + 100000, seats[:1])

    assert response.status_code == 404


async def test_validation(http, user, event):
    event_id, seats = event

    for bad in ([], [seats[0], seats[0]], list(range(1, 10))):
        response = await http.put(
            f"/holds/{uuid.uuid4()}", json={"event_id": event_id, "seat_ids": bad}, headers=user
        )
        assert response.status_code == 422


async def test_fifty_people_one_seat(http, event, engine):
    event_id, seats = event
    users = await asyncio.gather(*(make_user(http) for _ in range(50)))

    results = await asyncio.gather(*(place(http, u, event_id, seats[:1]) for u in users))

    codes = sorted(r.status_code for _, r in results)
    assert codes == [201] + [409] * 49


async def test_requests_queued_behind_a_lock_cannot_all_win(http, event, engine):
    """Without the row lock, each one reads a free seat, then each one overwrites the last."""
    event_id, seats = event
    users = await asyncio.gather(*(make_user(http) for _ in range(8)))

    async with AsyncSession(engine) as blocker:
        async with blocker.begin():
            await blocker.execute(
                text("SELECT id FROM seats WHERE id = :s FOR UPDATE"), {"s": seats[0]}
            )
            waiting = [asyncio.create_task(place(http, u, event_id, seats[:1])) for u in users]
            await asyncio.sleep(1)

    results = await asyncio.gather(*waiting)
    assert sorted(r.status_code for _, r in results) == [201] + [409] * 7


async def test_overlapping_groups_in_opposite_orders_do_not_deadlock(http, user, event):
    event_id, seats = event
    forward, backward = seats[:6], list(reversed(seats[:6]))

    results = await asyncio.gather(
        *(place(http, user, event_id, forward if n % 2 else backward) for n in range(40))
    )

    codes = [r.status_code for _, r in results]
    assert set(codes) <= {201, 409}
    assert codes.count(201) == 1


async def test_expiry_is_judged_when_the_lock_is_won(http, user, other, event, engine):
    """A request queued behind a lock must not use the time it arrived."""
    event_id, seats = event
    hold_id, _ = await place(http, user, event_id, seats[:1])
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE seats SET hold_expires_at = clock_timestamp() + interval '700 ms' "
                "WHERE hold_id = :h"
            ),
            {"h": hold_id},
        )

    async with AsyncSession(engine) as blocker:
        async with blocker.begin():
            await blocker.execute(
                text("SELECT id FROM seats WHERE id = :s FOR UPDATE"), {"s": seats[0]}
            )
            waiting = asyncio.create_task(place(http, other, event_id, seats[:1]))
            await asyncio.sleep(1.2)

    _, response = await waiting
    assert response.status_code == 201


async def test_group_shares_one_expiry(http, user, event, engine):
    event_id, seats = event
    await place(http, user, event_id, seats[:4])

    expiries = {(await read_seat(engine, s)).hold_expires_at for s in seats[:4]}

    assert len(expiries) == 1
