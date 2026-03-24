# =============================================================================
# database.py — SQLAlchemy async engine + ORM models
# =============================================================================
# Uses SQLAlchemy 2.x with asyncpg driver for non-blocking DB access.
# All Gold-layer tables are mirrored here so FastAPI can query them directly.
# =============================================================================

from datetime import date, datetime
from typing import AsyncGenerator

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime,
    Float, Integer, String, text
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

# ---------------------------------------------------------------------------
# Engine — pool_pre_ping keeps connections alive behind a load balancer
# ---------------------------------------------------------------------------
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass


# ===========================================================================
# ORM Models — mirror of the Gold Delta tables loaded by the ingestion job
# ===========================================================================

class DailyRevenue(Base):
    """
    Gold table: aggregated daily revenue per store region.
    Populated by the Databricks pipeline; read-only from the API.
    """
    __tablename__ = "daily_revenue"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    date                = Column(Date, nullable=False, index=True)
    store_region        = Column(String(100), nullable=False, index=True)
    total_revenue       = Column(Float, nullable=False)
    gross_revenue       = Column(Float, nullable=False)
    total_discounts     = Column(Float, nullable=False)
    units_sold          = Column(Integer, nullable=False)
    transaction_count   = Column(Integer, nullable=False)
    unique_customers    = Column(Integer, nullable=False)
    avg_basket_value    = Column(Float, nullable=False)
    processed_at        = Column(DateTime, nullable=False, default=datetime.utcnow)


class ProductPerformance(Base):
    """
    Gold table: lifetime revenue and volume metrics per product.
    """
    __tablename__ = "product_performance"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    product_id          = Column(String(20), nullable=False, unique=True, index=True)
    product_name        = Column(String(200), nullable=False)
    category            = Column(String(100), nullable=False, index=True)
    total_revenue       = Column(Float, nullable=False)
    total_units_sold    = Column(Integer, nullable=False)
    avg_selling_price   = Column(Float, nullable=False)
    avg_discount_pct    = Column(Float, nullable=False)
    total_transactions  = Column(Integer, nullable=False)
    stores_selling      = Column(Integer, nullable=False)
    revenue_rank        = Column(Integer, nullable=False)
    processed_at        = Column(DateTime, nullable=False, default=datetime.utcnow)


class StorePerformance(Base):
    """
    Gold table: revenue and engagement metrics per store.
    """
    __tablename__ = "store_performance"

    id                      = Column(Integer, primary_key=True, autoincrement=True)
    store_id                = Column(String(20), nullable=False, unique=True, index=True)
    store_name              = Column(String(200), nullable=False)
    store_region            = Column(String(100), nullable=False, index=True)
    total_revenue           = Column(Float, nullable=False)
    total_units_sold        = Column(Integer, nullable=False)
    total_transactions      = Column(Integer, nullable=False)
    unique_customers        = Column(Integer, nullable=False)
    avg_transaction_value   = Column(Float, nullable=False)
    unique_products_sold    = Column(Integer, nullable=False)
    revenue_rank            = Column(Integer, nullable=False)
    processed_at            = Column(DateTime, nullable=False, default=datetime.utcnow)


class CategoryTrend(Base):
    """
    Gold table: weekly revenue roll-up per product category.
    """
    __tablename__ = "category_trends"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    year                = Column(Integer, nullable=False, index=True)
    week                = Column(Integer, nullable=False, index=True)
    category            = Column(String(100), nullable=False, index=True)
    weekly_revenue      = Column(Float, nullable=False)
    weekly_units_sold   = Column(Integer, nullable=False)
    distinct_products   = Column(Integer, nullable=False)
    processed_at        = Column(DateTime, nullable=False, default=datetime.utcnow)


# ===========================================================================
# Dependency — FastAPI injects this into every route
# ===========================================================================
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Yield a database session per request, automatically rolling back
    on exception and closing on completion.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ===========================================================================
# DB Initialisation helper (called on app startup)
# ===========================================================================
async def init_db() -> None:
    """
    Create tables using IF NOT EXISTS — atomic and race-safe.
    Safe to call from multiple workers or on repeated restarts.
    """
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS daily_revenue (
                id                SERIAL PRIMARY KEY,
                date              DATE NOT NULL,
                store_region      VARCHAR(100) NOT NULL,
                total_revenue     FLOAT NOT NULL,
                gross_revenue     FLOAT NOT NULL,
                total_discounts   FLOAT NOT NULL,
                units_sold        INTEGER NOT NULL,
                transaction_count INTEGER NOT NULL,
                unique_customers  INTEGER NOT NULL,
                avg_basket_value  FLOAT NOT NULL,
                processed_at      TIMESTAMP NOT NULL
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS product_performance (
                id                  SERIAL PRIMARY KEY,
                product_id          VARCHAR(20) UNIQUE NOT NULL,
                product_name        VARCHAR(200) NOT NULL,
                category            VARCHAR(100) NOT NULL,
                total_revenue       FLOAT NOT NULL,
                total_units_sold    INTEGER NOT NULL,
                avg_selling_price   FLOAT NOT NULL,
                avg_discount_pct    FLOAT NOT NULL,
                total_transactions  INTEGER NOT NULL,
                stores_selling      INTEGER NOT NULL,
                revenue_rank        INTEGER NOT NULL,
                processed_at        TIMESTAMP NOT NULL
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS store_performance (
                id                    SERIAL PRIMARY KEY,
                store_id              VARCHAR(20) UNIQUE NOT NULL,
                store_name            VARCHAR(200) NOT NULL,
                store_region          VARCHAR(100) NOT NULL,
                total_revenue         FLOAT NOT NULL,
                total_units_sold      INTEGER NOT NULL,
                total_transactions    INTEGER NOT NULL,
                unique_customers      INTEGER NOT NULL,
                avg_transaction_value FLOAT NOT NULL,
                unique_products_sold  INTEGER NOT NULL,
                revenue_rank          INTEGER NOT NULL,
                processed_at          TIMESTAMP NOT NULL
            )
        """))
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS category_trends (
                id                SERIAL PRIMARY KEY,
                year              INTEGER NOT NULL,
                week              INTEGER NOT NULL,
                category          VARCHAR(100) NOT NULL,
                weekly_revenue    FLOAT NOT NULL,
                weekly_units_sold INTEGER NOT NULL,
                distinct_products INTEGER NOT NULL,
                processed_at      TIMESTAMP NOT NULL
            )
        """))
    print("Database tables verified")