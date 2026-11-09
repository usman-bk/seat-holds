from datetime import UTC, datetime, timedelta

import jwt

from app.config import settings


def bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def test_no_token(http):
    assert (
        await http.post("/holds/00000000-0000-0000-0000-000000000000/confirm")
    ).status_code == 401


async def test_wrong_password(http):
    await http.post("/register", json={"email": "wrong@example.com", "password": "a long password"})

    response = await http.post("/token", data={"username": "wrong@example.com", "password": "nope"})

    assert response.status_code == 401


async def test_unknown_email_looks_like_a_wrong_password(http):
    response = await http.post("/token", data={"username": "ghost@example.com", "password": "x"})

    assert response.status_code == 401
    assert response.json()["detail"] == "wrong email or password"


async def test_email_taken(http):
    body = {"email": "dup@example.com", "password": "a long password"}
    await http.post("/register", json=body)

    assert (await http.post("/register", json=body)).status_code == 409


async def test_expired_token(http):
    token = jwt.encode(
        {"sub": "1", "exp": datetime.now(UTC) - timedelta(minutes=1)}, settings.jwt_secret, "HS256"
    )

    response = await http.delete(
        "/holds/00000000-0000-0000-0000-000000000000", headers=bearer(token)
    )

    assert response.status_code == 401


async def test_token_signed_with_another_key(http):
    token = jwt.encode(
        {"sub": "1", "exp": datetime.now(UTC) + timedelta(minutes=5)}, "x" * 40, "HS256"
    )

    response = await http.delete(
        "/holds/00000000-0000-0000-0000-000000000000", headers=bearer(token)
    )

    assert response.status_code == 401


async def test_unsigned_token(http):
    token = jwt.encode({"sub": "1", "exp": 9999999999}, "", algorithm="none")

    response = await http.delete(
        "/holds/00000000-0000-0000-0000-000000000000", headers=bearer(token)
    )

    assert response.status_code == 401
