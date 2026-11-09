import os
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from app.db import Base, Event, Seat

ROOT = Path(__file__).resolve().parent.parent
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/seat_holds_test"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/9")
HOLD_SECONDS = 600

assert DATABASE_URL.rsplit("?")[0].endswith("_test"), "these tests drop every table"


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _start(port: int, **extra: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "DATABASE_URL": DATABASE_URL,
        "REDIS_URL": REDIS_URL,
        "HOLD_SECONDS": str(HOLD_SECONDS),
        "SWEEP_SECONDS": "0.2",
        **extra,
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
    )
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=1).status_code == 200:
                return process
        except httpx.HTTPError:
            time.sleep(0.1)
    process.kill()
    raise RuntimeError("server did not start")


def _stop(process: subprocess.Popen) -> None:
    process.terminate()
    try:
        process.wait(10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


@pytest_asyncio.fixture(scope="session")
async def engine():
    created = create_async_engine(DATABASE_URL, poolclass=NullPool)
    async with created.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    yield created
    await created.dispose()


@dataclass
class Servers:
    a: str
    b: str


@pytest.fixture(scope="session")
def servers(engine):
    port_a, port_b = _free_port(), _free_port()
    first = _start(port_a)
    second = _start(port_b)
    yield Servers(f"http://127.0.0.1:{port_a}", f"http://127.0.0.1:{port_b}")
    _stop(first)
    _stop(second)


@pytest_asyncio.fixture
async def http(servers):
    limits = httpx.Limits(max_connections=200)
    async with httpx.AsyncClient(base_url=servers.a, limits=limits, timeout=30) as client:
        yield client


@pytest_asyncio.fixture
async def event(engine):
    """An event with twelve seats, returned as (event_id, [seat ids])."""
    async with AsyncSession(engine, expire_on_commit=False) as session:
        async with session.begin():
            row = Event(name=f"event {uuid.uuid4()}")
            session.add(row)
            await session.flush()
            seats = [Seat(event_id=row.id, label=f"A{n}") for n in range(12)]
            session.add_all(seats)
            await session.flush()
            return row.id, [s.id for s in seats]


async def make_user(http: httpx.AsyncClient) -> dict:
    email = f"{uuid.uuid4().hex}@example.com"
    password = "correct horse battery"
    assert (
        await http.post("/register", json={"email": email, "password": password})
    ).status_code == 201
    response = await http.post("/token", data={"username": email, "password": password})
    assert response.status_code == 200
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest_asyncio.fixture
async def user(http):
    return await make_user(http)


@pytest_asyncio.fixture
async def other(http):
    return await make_user(http)


async def place(http, headers, event_id, seat_ids, hold_id=None):
    hold_id = hold_id or uuid.uuid4()
    response = await http.put(
        f"/holds/{hold_id}", json={"event_id": event_id, "seat_ids": seat_ids}, headers=headers
    )
    return hold_id, response


async def expire(engine, hold_id) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "UPDATE seats SET hold_expires_at = clock_timestamp() - interval '1 second' "
                "WHERE hold_id = :h"
            ),
            {"h": hold_id},
        )


async def read_seat(engine, seat_id) -> Seat:
    async with AsyncSession(engine, expire_on_commit=False) as session:
        return await session.scalar(select(Seat).where(Seat.id == seat_id))
