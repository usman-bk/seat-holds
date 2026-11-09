import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field, field_validator
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from . import auth, holds
from .config import settings
from .db import Session, User, engine, get_session
from .live import Hub

log = logging.getLogger("seat_holds")
DbSession = Annotated[AsyncSession, Depends(get_session)]


async def _sweeper(hub: Hub) -> None:
    while True:
        await asyncio.sleep(settings.sweep_seconds)
        try:
            async with Session() as session:
                changes = await holds.sweep_expired(session)
            if changes:
                await hub.publish(changes)
        except Exception:
            log.exception("sweep failed")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    hub = Hub(redis)
    hub.start()
    app.state.hub = hub
    sweeper = asyncio.create_task(_sweeper(hub))
    yield
    sweeper.cancel()
    await asyncio.gather(sweeper, return_exceptions=True)
    await hub.stop()
    await redis.aclose()
    await engine.dispose()


app = FastAPI(title="seat-holds", lifespan=lifespan)


class Credentials(BaseModel):
    email: str = Field(max_length=254, pattern=r"^[^@\s]+@[^@\s]+$")
    password: str = Field(min_length=10, max_length=200)


class HoldRequest(BaseModel):
    event_id: int
    seat_ids: list[int] = Field(min_length=1, max_length=8)

    @field_validator("seat_ids")
    @classmethod
    def no_repeats(cls, value: list[int]) -> list[int]:
        if len(set(value)) != len(value):
            raise ValueError("a seat is listed twice")
        return value


class HoldOut(BaseModel):
    hold_id: UUID
    expires_at: datetime
    seat_ids: list[int]


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


@app.post("/register", status_code=status.HTTP_201_CREATED)
async def register(body: Credentials, session: DbSession) -> dict:
    password_hash = await auth.hash_password(body.password)
    try:
        async with session.begin():
            session.add(User(email=body.email.lower(), password_hash=password_hash))
    except IntegrityError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "email already registered") from exc
    return {"email": body.email.lower()}


@app.post("/token")
async def token(form: Annotated[OAuth2PasswordRequestForm, Depends()], session: DbSession) -> dict:
    user = await session.scalar(select(User).where(User.email == form.username.lower()))
    # Hand the connection back before Argon2 runs. A login should not occupy
    # a pooled connection for the length of a password hash.
    await session.close()
    if not await auth.verify_password(user.password_hash if user else None, form.password):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "wrong email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"access_token": auth.issue_token(user.id), "token_type": "bearer"}


@app.put("/holds/{hold_id}", status_code=status.HTTP_201_CREATED)
async def put_hold(
    hold_id: UUID, body: HoldRequest, user: auth.CurrentUser, session: DbSession, request: Request
) -> HoldOut:
    try:
        result = await holds.place_hold(
            session, event_id=body.event_id, seat_ids=body.seat_ids, user_id=user, hold_id=hold_id
        )
    except holds.UnknownSeats as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such seat at this event") from exc
    except holds.SeatsUnavailable as exc:
        return JSONResponse(  # type: ignore[return-value]
            {"detail": "seats unavailable", "seat_ids": exc.seat_ids},
            status.HTTP_409_CONFLICT,
        )
    await request.app.state.hub.publish(result.changes)
    return HoldOut(hold_id=result.hold_id, expires_at=result.expires_at, seat_ids=result.seat_ids)


@app.post("/holds/{hold_id}/confirm")
async def confirm(
    hold_id: UUID, user: auth.CurrentUser, session: DbSession, request: Request
) -> dict:
    try:
        changes = await holds.confirm_hold(session, hold_id=hold_id, user_id=user)
    except holds.HoldGone as exc:
        raise HTTPException(status.HTTP_410_GONE, "this hold has expired") from exc
    await request.app.state.hub.publish(changes)
    return {"confirmed": True}


@app.delete("/holds/{hold_id}", status_code=status.HTTP_204_NO_CONTENT)
async def release(
    hold_id: UUID, user: auth.CurrentUser, session: DbSession, request: Request
) -> None:
    changes = await holds.release_hold(session, hold_id=hold_id, user_id=user)
    await request.app.state.hub.publish(changes)


@app.get("/events/{event_id}/seats")
async def seats(event_id: int, session: DbSession) -> list[dict]:
    rows, found = await holds.snapshot(session, event_id)
    if not found:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such event")
    return rows


@app.websocket("/events/{event_id}/live")
async def live(socket: WebSocket, event_id: int) -> None:
    hub: Hub = socket.app.state.hub
    await socket.accept()
    # Join first, snapshot second. The other order loses any change made in
    # the gap. This order can repeat one, and the versions sort that out.
    client = hub.join(event_id)
    overflow = asyncio.create_task(client.overflowed.wait())
    try:
        # Its own short session, never one held for the life of the socket.
        # A hundred viewers must not cost a hundred pooled connections.
        async with Session() as session:
            rows, found = await holds.snapshot(session, event_id)
        if not found:
            await socket.close(code=4404)
            return
        await socket.send_json({"type": "snapshot", "seats": rows})

        while True:
            next_message = asyncio.create_task(client.queue.get())
            done, _ = await asyncio.wait(
                {next_message, overflow}, return_when=asyncio.FIRST_COMPLETED
            )
            if overflow in done:
                next_message.cancel()
                await socket.close(code=1013, reason="fell behind, reconnect")
                return
            await socket.send_json({"type": "change", **next_message.result()})
    except WebSocketDisconnect:
        pass
    finally:
        overflow.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await overflow
        hub.leave(event_id, client)
