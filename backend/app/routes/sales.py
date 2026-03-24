# =============================================================================
# routes/sales.py — Analytics API endpoints
# =============================================================================
# All endpoints are async, paginated where appropriate, and return typed
# Pydantic response models so the OpenAPI docs are always accurate.
# =============================================================================

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import (
    get_db,
    DailyRevenue,
    ProductPerformance,
    StorePerformance,
    CategoryTrend,
)

router = APIRouter(prefix="/api/v1/sales", tags=["Sales Analytics"])


# ===========================================================================
# Response Schemas
# ===========================================================================

class DailyRevenueOut(BaseModel):
    date:               date
    store_region:       str
    total_revenue:      float
    gross_revenue:      float
    total_discounts:    float
    units_sold:         int
    transaction_count:  int
    unique_customers:   int
    avg_basket_value:   float

    model_config = {"from_attributes": True}


class ProductPerformanceOut(BaseModel):
    product_id:         str
    product_name:       str
    category:           str
    total_revenue:      float
    total_units_sold:   int
    avg_selling_price:  float
    avg_discount_pct:   float
    total_transactions: int
    stores_selling:     int
    revenue_rank:       int

    model_config = {"from_attributes": True}


class StorePerformanceOut(BaseModel):
    store_id:               str
    store_name:             str
    store_region:           str
    total_revenue:          float
    total_units_sold:       int
    total_transactions:     int
    unique_customers:       int
    avg_transaction_value:  float
    unique_products_sold:   int
    revenue_rank:           int

    model_config = {"from_attributes": True}


class CategoryTrendOut(BaseModel):
    year:               int
    week:               int
    category:           str
    weekly_revenue:     float
    weekly_units_sold:  int
    distinct_products:  int

    model_config = {"from_attributes": True}


class SummaryOut(BaseModel):
    total_revenue:          float
    total_transactions:     int
    total_units_sold:       int
    unique_customers:       int
    avg_basket_value:       float
    top_region:             str
    top_product:            str
    top_store:              str


class PaginatedResponse(BaseModel):
    total:  int
    page:   int
    size:   int
    data:   list


# ===========================================================================
# Endpoints
# ===========================================================================

@router.get(
    "/summary",
    response_model=SummaryOut,
    summary="Executive KPI summary",
    description="Returns top-level KPIs across the entire dataset — ideal for a dashboard header card.",
)
async def get_summary(db: AsyncSession = Depends(get_db)) -> SummaryOut:
    """
    Aggregates all Gold tables into a single summary object.
    Demonstrates multi-table async queries inside one endpoint.
    """
    # Revenue totals from daily_revenue
    rev_result = await db.execute(
        select(
            func.sum(DailyRevenue.total_revenue).label("total_revenue"),
            func.sum(DailyRevenue.transaction_count).label("total_transactions"),
            func.sum(DailyRevenue.units_sold).label("total_units_sold"),
            func.sum(DailyRevenue.unique_customers).label("unique_customers"),
            func.avg(DailyRevenue.avg_basket_value).label("avg_basket_value"),
        )
    )
    rev = rev_result.one()

    # Top region by revenue
    region_result = await db.execute(
        select(DailyRevenue.store_region)
        .group_by(DailyRevenue.store_region)
        .order_by(desc(func.sum(DailyRevenue.total_revenue)))
        .limit(1)
    )
    top_region = region_result.scalar_one_or_none() or "N/A"

    # Top product by revenue
    product_result = await db.execute(
        select(ProductPerformance.product_name)
        .order_by(ProductPerformance.revenue_rank)
        .limit(1)
    )
    top_product = product_result.scalar_one_or_none() or "N/A"

    # Top store by revenue
    store_result = await db.execute(
        select(StorePerformance.store_name)
        .order_by(StorePerformance.revenue_rank)
        .limit(1)
    )
    top_store = store_result.scalar_one_or_none() or "N/A"

    return SummaryOut(
        total_revenue=round(rev.total_revenue or 0, 2),
        total_transactions=rev.total_transactions or 0,
        total_units_sold=rev.total_units_sold or 0,
        unique_customers=rev.unique_customers or 0,
        avg_basket_value=round(rev.avg_basket_value or 0, 2),
        top_region=top_region,
        top_product=top_product,
        top_store=top_store,
    )


@router.get(
    "/daily-revenue",
    response_model=PaginatedResponse,
    summary="Daily revenue trend",
    description="Paginated daily revenue with optional date-range and region filters.",
)
async def get_daily_revenue(
    start_date:  Optional[date] = Query(None, description="Filter from this date (inclusive)"),
    end_date:    Optional[date] = Query(None, description="Filter to this date (inclusive)"),
    region:      Optional[str]  = Query(None, description="Filter by store region"),
    page:        int            = Query(1, ge=1),
    size:        int            = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> PaginatedResponse:
    stmt = select(DailyRevenue).order_by(DailyRevenue.date.desc())

    if start_date:
        stmt = stmt.where(DailyRevenue.date >= start_date)
    if end_date:
        stmt = stmt.where(DailyRevenue.date <= end_date)
    if region:
        stmt = stmt.where(DailyRevenue.store_region.ilike(f"%{region}%"))

    total = await _count(db, stmt)
    rows  = (await db.execute(stmt.offset((page - 1) * size).limit(size))).scalars().all()

    return PaginatedResponse(
        total=total, page=page, size=size,
        data=[DailyRevenueOut.model_validate(r) for r in rows],
    )


@router.get(
    "/top-products",
    response_model=List[ProductPerformanceOut],
    summary="Top-selling products",
    description="Returns the N best-performing products ranked by net revenue.",
)
async def get_top_products(
    limit:    int           = Query(10, ge=1, le=50, description="Number of products to return"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    db: AsyncSession = Depends(get_db),
) -> List[ProductPerformanceOut]:
    stmt = (
        select(ProductPerformance)
        .order_by(ProductPerformance.revenue_rank)
        .limit(limit)
    )
    if category:
        stmt = stmt.where(ProductPerformance.category.ilike(f"%{category}%"))

    rows = (await db.execute(stmt)).scalars().all()
    return [ProductPerformanceOut.model_validate(r) for r in rows]


@router.get(
    "/store-performance",
    response_model=List[StorePerformanceOut],
    summary="Store performance leaderboard",
    description="All stores ranked by total net revenue, with optional region filter.",
)
async def get_store_performance(
    region: Optional[str] = Query(None, description="Filter by store region"),
    db: AsyncSession = Depends(get_db),
) -> List[StorePerformanceOut]:
    stmt = select(StorePerformance).order_by(StorePerformance.revenue_rank)

    if region:
        stmt = stmt.where(StorePerformance.store_region.ilike(f"%{region}%"))

    rows = (await db.execute(stmt)).scalars().all()
    return [StorePerformanceOut.model_validate(r) for r in rows]


@router.get(
    "/store/{store_id}",
    response_model=StorePerformanceOut,
    summary="Single store detail",
    description="Returns performance metrics for one specific store.",
)
async def get_store_detail(
    store_id: str,
    db: AsyncSession = Depends(get_db),
) -> StorePerformanceOut:
    result = await db.execute(
        select(StorePerformance).where(StorePerformance.store_id == store_id.upper())
    )
    store = result.scalar_one_or_none()
    if not store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Store '{store_id}' not found.",
        )
    return StorePerformanceOut.model_validate(store)


@router.get(
    "/category-trends",
    response_model=List[CategoryTrendOut],
    summary="Weekly category revenue trends",
    description="Weekly roll-up of revenue and volume per product category.",
)
async def get_category_trends(
    year:     Optional[int] = Query(None, description="Filter by year, e.g. 2024"),
    category: Optional[str] = Query(None, description="Filter by category name"),
    db: AsyncSession = Depends(get_db),
) -> List[CategoryTrendOut]:
    stmt = select(CategoryTrend).order_by(
        CategoryTrend.year.desc(),
        CategoryTrend.week.desc(),
        CategoryTrend.weekly_revenue.desc(),
    )
    if year:
        stmt = stmt.where(CategoryTrend.year == year)
    if category:
        stmt = stmt.where(CategoryTrend.category.ilike(f"%{category}%"))

    rows = (await db.execute(stmt)).scalars().all()
    return [CategoryTrendOut.model_validate(r) for r in rows]


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
async def _count(db: AsyncSession, stmt) -> int:
    """Return the total row count for a SELECT statement (used for pagination)."""
    count_stmt = select(func.count()).select_from(stmt.subquery())
    return (await db.execute(count_stmt)).scalar_one()