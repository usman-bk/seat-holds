from collections.abc import AsyncIterator
from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, String, UniqueConstraint, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .config import settings

engine = create_async_engine(settings.database_url, pool_size=settings.pool_size, max_overflow=0)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    email: Mapped[str] = mapped_column(String(254), unique=True)
    password_hash: Mapped[str] = mapped_column(String(200))


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(200))


class Seat(Base):
    __tablename__ = "seats"
    __table_args__ = (
        UniqueConstraint("event_id", "label"),
        Index(
            "seats_active_hold_idx",
            "hold_expires_at",
            postgresql_where=text("hold_id IS NOT NULL"),
        ),
        Index("seats_hold_idx", "hold_id", postgresql_where=text("hold_id IS NOT NULL")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[int] = mapped_column(ForeignKey("events.id"))
    label: Mapped[str] = mapped_column(String(16))
    version: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")

    hold_id: Mapped[UUID | None]
    holder_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"))
    hold_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    sold_to: Mapped[int | None] = mapped_column(ForeignKey("users.id"))
    sold_under: Mapped[UUID | None]


async def get_session() -> AsyncIterator[AsyncSession]:
    async with Session() as session:
        yield session
