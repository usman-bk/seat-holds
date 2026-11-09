# seat-holds

_Built with AI assistance._

Holds seats while someone pays, so a seat cannot be sold twice, a group gets all
of its seats or none, and everyone looking at the seat map sees it change.

FastAPI, async SQLAlchemy 2 on asyncpg, PostgreSQL, Redis pub/sub, WebSockets,
OAuth2 password flow with JWT and Argon2. The tests start two real uvicorn
processes and talk to them over HTTP and WebSocket.

```
app/holds.py   every rule about who may have a seat
app/live.py    one Redis subscription per process, fanned out to its sockets
app/auth.py    tokens, and password hashing off the event loop
app/main.py    routes and the WebSocket handler
```

## Why the hold is a PUT with the client's id

    PUT /holds/8f14e45f-ceea-467f-a0e6-1f6bd2d2d1f5
    {"event_id": 7, "seat_ids": [31, 32]}

The phone sends this on a bad connection, the seats get held, and the response
never arrives. With a POST the retry finds the seats taken, by the same person,
who is then told their own seats are gone.

Because the client picks the id, the retry is the same request. It returns the
same hold with the same expiry. It does not extend it, otherwise retrying would
be a way to keep a seat forever.

## Three ways the clock or the lock can lie

**`now()` is the wrong time.** In Postgres it is frozen when the transaction
starts. A request that waited a second behind a row lock would judge expiry by
when it arrived, and refuse a seat that has already come free. The code reads
`clock_timestamp()` after winning the lock.
`test_expiry_is_judged_when_the_lock_is_won` holds the lock past the expiry.

**Expiry is computed once per group.** Written as a per row SQL expression it
differs by microseconds between seats, so a group could half expire at the
boundary and be half confirmed.

**Seats are locked in id order.** A group of six in one order and a group of six
in another deadlock, and Postgres kills one with a 500. When a test shuffles
the lock order, the deadlock test fails.

## An expired hold needs nothing to run

A seat is free when its hold has passed. No job has to clear it first, so if the
sweeper is stopped, nobody is refused a seat they should get.

The sweeper exists only to tell the seat maps. It uses `SKIP LOCKED`, so with two
processes each expired seat is announced once.

## The live seat map

Each process keeps one Redis subscription and passes changes to its own sockets.
A hold placed through one process reaches a viewer connected to the other.

- The socket joins the hub before reading the snapshot. The other order loses
  whatever changes in the gap. This order can repeat a change, and every seat
  carries a version so a client can ignore the older one.
- The snapshot uses a short session of its own. Sixty open sockets against a pool
  of ten still leave the API responsive, and that is a test.
- A client that falls 256 messages behind is closed with 1013 and reconnects to a
  new snapshot, so one slow phone cannot delay the rest.
- Postgres commits before Redis hears anything. If Redis is down the hold still
  succeeds, and the maps catch up on reconnect.

## Argon2 and the event loop

Argon2 is slow on purpose, and on the event loop it would freeze every socket in
the process for each login. It runs in a thread, and login gives its database
connection back first so a burst of logins cannot drain the pool.

## Two tests that passed when they should not have

Breaking the code on purpose found these. Each one looked fine and proved nothing.

The race test sent fifty people at one seat and got one winner, even with the row
lock removed, because the reads and writes were too quick to collide. It now
holds the row while eight requests queue up, which makes every one of them read
a free seat if the lock is missing.

The event loop test passed with Argon2 running directly on the loop. It measured
the gap between ticks, and the ticker was cancelled before it could record the
gap it had been stuck in.

Each of these was then applied by hand and turned the suite red:

| Deliberate break | Failed |
|---|---|
| `now()` for `clock_timestamp()` | lock timing test |
| Remove `FOR UPDATE` | queued requests test, lock timing test |
| Lock seats in random order | deadlock test |
| Compute expiry per row | one expiry test, repeat PUT test |
| Hash on the event loop | loop tests |
| Socket holds a connection open | sixty viewers test |
| Remove the repeat PUT check | repeat PUT test |
| Remove the holder check on confirm | holder test |
| Skip signature verification | three token tests |

CI repeats the first of these on every push.

## API

| | |
|---|---|
| `POST /register`, `POST /token` | Account and OAuth2 password login |
| `PUT /holds/{uuid}` | 201 with the expiry, 409 with the seats that were taken |
| `POST /holds/{uuid}/confirm` | 200, safe to repeat. 410 once expired |
| `DELETE /holds/{uuid}` | Release |
| `GET /events/{id}/seats` | The map |
| `WS /events/{id}/live` | Snapshot, then changes |

## Run the tests

Postgres and Redis on localhost, plus [uv](https://docs.astral.sh/uv/).

    createdb seat_holds_test
    uv sync
    uv run pytest

39 tests, about 32 seconds. `DATABASE_URL` and `REDIS_URL` override the defaults
in `tests/conftest.py`. The database name must end in `_test`, because the suite
drops every table in it.

## Gaps

- No payment. Confirm is the point where a payment provider would plug in.
- Tables come from `python -m app.init_db`. There are no migrations yet.
- Nothing rate limits holds, so one account can sit on seats for ten minutes at a
  time. That needs a per user cap.
- Two paths have no test: the join-before-snapshot ordering, and dropping sockets
  when the Redis subscription is lost.
