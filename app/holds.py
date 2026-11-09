from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .db import Seat


class SeatsUnavailable(Exception):
    def __init__(self, seat_ids: list[int]):
        self.seat_ids = seat_ids


class UnknownSeats(Exception):
    pass


class HoldGone(Exception):
    pass


@dataclass(frozen=True)
class Change:
    seat_id: int
    event_id: int
    version: int
    state: str


@dataclass(frozen=True)
class HoldResult:
    hold_id: UUID
    expires_at: datetime
    seat_ids: list[int]
    changes: list[Change]


async def _clock(session: AsyncSession) -> datetime:
    # clock_timestamp, not now(). now() is frozen when the transaction starts,
    # so a request that queued behind a lock would judge expiry by the moment
    # it arrived rather than the moment it got the row.
    return await session.scalar(select(func.clock_timestamp()))


async def place_hold(
    session: AsyncSession, *, event_id: int, seat_ids: list[int], user_id: int, hold_id: UUID
) -> HoldResult:
    wanted = sorted(set(seat_ids))
    async with session.begin():
        # Locked in id order. Two groups asking for overlapping seats in
        # different orders would otherwise deadlock each other.
        rows = (
            (
                await session.execute(
                    select(Seat)
                    .where(Seat.event_id == event_id, Seat.id.in_(wanted))
                    .order_by(Seat.id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        if len(rows) != len(wanted):
            raise UnknownSeats

        now = await _clock(session)

        def held(seat: Seat) -> bool:
            return seat.hold_id is not None and seat.hold_expires_at > now

        # The same PUT again, after a lost response, gets the same hold back
        # and does not get a longer one.
        if all(held(s) and s.hold_id == hold_id and s.holder_id == user_id for s in rows):
            return HoldResult(hold_id, rows[0].hold_expires_at, wanted, [])

        taken = [s.id for s in rows if s.sold_to is not None or held(s)]
        if taken:
            raise SeatsUnavailable(taken)

        # One expiry for the whole group, computed once. Evaluated per row it
        # would differ by microseconds and a group could half expire.
        expires_at = now + timedelta(seconds=settings.hold_seconds)
        updated = await session.execute(
            update(Seat)
            .where(Seat.id.in_(wanted))
            .values(
                hold_id=hold_id,
                holder_id=user_id,
                hold_expires_at=expires_at,
                version=Seat.version + 1,
            )
            .returning(Seat.id, Seat.version)
        )
        changes = [Change(sid, event_id, version, "held") for sid, version in updated.all()]
    return HoldResult(hold_id, expires_at, wanted, changes)


async def confirm_hold(session: AsyncSession, *, hold_id: UUID, user_id: int) -> list[Change]:
    async with session.begin():
        now = await _clock(session)
        updated = await session.execute(
            update(Seat)
            .where(
                Seat.hold_id == hold_id,
                Seat.holder_id == user_id,
                Seat.hold_expires_at > now,
                Seat.sold_to.is_(None),
            )
            .values(
                sold_to=user_id,
                sold_under=hold_id,
                hold_id=None,
                holder_id=None,
                hold_expires_at=None,
                version=Seat.version + 1,
            )
            .returning(Seat.id, Seat.event_id, Seat.version)
        )
        rows = updated.all()
        if rows:
            return [Change(sid, eid, version, "sold") for sid, eid, version in rows]

        already = await session.scalar(
            select(func.count())
            .select_from(Seat)
            .where(Seat.sold_under == hold_id, Seat.sold_to == user_id)
        )
        if already:
            return []
    raise HoldGone


async def release_hold(session: AsyncSession, *, hold_id: UUID, user_id: int) -> list[Change]:
    async with session.begin():
        updated = await session.execute(
            update(Seat)
            .where(Seat.hold_id == hold_id, Seat.holder_id == user_id)
            .values(hold_id=None, holder_id=None, hold_expires_at=None, version=Seat.version + 1)
            .returning(Seat.id, Seat.event_id, Seat.version)
        )
        return [Change(sid, eid, version, "free") for sid, eid, version in updated.all()]


_SWEEP = text(
    """
    WITH expired AS (
        SELECT id FROM seats
        WHERE hold_id IS NOT NULL AND hold_expires_at <= clock_timestamp()
        ORDER BY hold_expires_at
        LIMIT 500
        FOR UPDATE SKIP LOCKED
    )
    UPDATE seats s
    SET hold_id = NULL, holder_id = NULL, hold_expires_at = NULL, version = s.version + 1
    FROM expired WHERE s.id = expired.id
    RETURNING s.id, s.event_id, s.version
    """
)


async def sweep_expired(session: AsyncSession) -> list[Change]:
    """Tell people about expired holds. Availability never depends on this running."""
    async with session.begin():
        rows = (await session.execute(_SWEEP)).all()
    return [Change(sid, eid, version, "free") for sid, eid, version in rows]


def seat_state(seat: Seat, now: datetime) -> str:
    if seat.sold_to is not None:
        return "sold"
    if seat.hold_id is not None and seat.hold_expires_at > now:
        return "held"
    return "free"


async def snapshot(session: AsyncSession, event_id: int) -> tuple[list[dict], bool]:
    async with session.begin():
        now = await _clock(session)
        seats = (
            (await session.execute(select(Seat).where(Seat.event_id == event_id).order_by(Seat.id)))
            .scalars()
            .all()
        )
    return (
        [
            {"id": s.id, "label": s.label, "version": s.version, "state": seat_state(s, now)}
            for s in seats
        ],
        bool(seats),
    )
