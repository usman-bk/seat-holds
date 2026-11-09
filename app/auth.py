import asyncio
from datetime import UTC, datetime, timedelta
from typing import Annotated

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import VerificationError
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from .config import settings

_hasher = PasswordHasher()
_scheme = OAuth2PasswordBearer(tokenUrl="/token")
_ALGORITHM = "HS256"
# Verified when the email is unknown, so a login for a real account and a
# login for a made up one take the same time.
_DUMMY_HASH = _hasher.hash("not a real password")


async def hash_password(password: str) -> str:
    return await asyncio.to_thread(_hasher.hash, password)


async def verify_password(stored: str | None, password: str) -> bool:
    # Argon2 takes tens of milliseconds of CPU on purpose. Run on the event
    # loop, every open WebSocket in this process would stop while it does.
    def check() -> bool:
        try:
            _hasher.verify(stored or _DUMMY_HASH, password)
        except VerificationError:
            return False
        return stored is not None

    return await asyncio.to_thread(check)


def issue_token(user_id: int) -> str:
    claims = {
        "sub": str(user_id),
        "exp": datetime.now(UTC) + timedelta(minutes=settings.jwt_minutes),
    }
    return jwt.encode(claims, settings.jwt_secret, algorithm=_ALGORITHM)


def current_user(token: Annotated[str, Depends(_scheme)]) -> int:
    try:
        claims = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[_ALGORITHM],
            options={"require": ["exp", "sub"]},
        )
        return int(claims["sub"])
    except (jwt.PyJWTError, ValueError) as exc:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


CurrentUser = Annotated[int, Depends(current_user)]
