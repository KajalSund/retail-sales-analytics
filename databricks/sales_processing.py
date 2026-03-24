# =============================================================================
# Retail Sales Analytics — Databricks PySpark Processing
# =============================================================================
# Architecture:
#   Bronze  → raw ingested data (no transformations, append-only)
#   Silver  → cleaned, validated, enriched data
#   Gold    → business-level aggregates ready for the analytics API
#
# Run this notebook in Databricks with a Delta Lake-enabled cluster.
# For local testing, install: pyspark, delta-spark
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# 1. Spark Session
#    In Databricks this session is pre-created; locally we build it here.
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("RetailSalesAnalytics")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Storage Paths  (replace with your ADLS Gen2 / DBFS paths in production)
# ---------------------------------------------------------------------------
BASE_PATH    = "/tmp/retail_sales"          # local dev path
BRONZE_PATH  = f"{BASE_PATH}/bronze/sales"
SILVER_PATH  = f"{BASE_PATH}/silver/sales"
GOLD_DAILY   = f"{BASE_PATH}/gold/daily_revenue"
GOLD_PRODUCT = f"{BASE_PATH}/gold/product_performance"
GOLD_STORE   = f"{BASE_PATH}/gold/store_performance"
GOLD_CATEGORY= f"{BASE_PATH}/gold/category_trends"

# ---------------------------------------------------------------------------
# 3. Explicit Schema — never infer schemas in production pipelines
# ---------------------------------------------------------------------------
SALES_SCHEMA = StructType([
    StructField("transaction_id", StringType(),  nullable=False),
    StructField("date",           StringType(),  nullable=False),   # cast later
    StructField("store_id",       StringType(),  nullable=False),
    StructField("store_name",     StringType(),  nullable=True),
    StructField("store_region",   StringType(),  nullable=True),
    StructField("product_id",     StringType(),  nullable=False),
    StructField("product_name",   StringType(),  nullable=True),
    StructField("category",       StringType(),  nullable=True),
    StructField("quantity",       IntegerType(), nullable=False),
    StructField("unit_price",     DoubleType(),  nullable=False),
    StructField("discount_pct",   DoubleType(),  nullable=True),
    StructField("customer_id",    StringType(),  nullable=True),
])

# ===========================================================================
# BRONZE LAYER — raw ingestion, no business logic
# ===========================================================================
def ingest_to_bronze(source_path: str) -> None:
    """
    Read raw CSV from Data Lake (ADF drops files here) and write to a
    Delta table partitioned by ingestion date.  Uses APPEND mode so we
    preserve a full audit trail of every file load.
    """
    print("▶ [BRONZE] Reading raw CSV ...")
    raw_df = (
        spark.read
        .option("header", "true")
        .schema(SALES_SCHEMA)
        .csv(source_path)
    )

    # Attach pipeline metadata columns
    bronze_df = raw_df.withColumns({
        "ingestion_timestamp": F.current_timestamp(),
        "source_file":         F.lit(source_path),
        "pipeline_run_date":   F.current_date(),
    })

    record_count = bronze_df.count()
    print(f"   ✓ {record_count:,} records read from source")

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .partitionBy("pipeline_run_date")
        .save(BRONZE_PATH)
    )
    print(f"   ✓ Bronze layer written → {BRONZE_PATH}")


# ===========================================================================
# SILVER LAYER — clean, validate, and enrich
# ===========================================================================
def transform_to_silver() -> None:
    """
    Apply data quality rules, type casts, and derived columns.
    Uses MERGE (upsert) to make this step idempotent — safe to re-run.
    """
    print("▶ [SILVER] Cleaning and enriching data ...")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    silver_df = (
        bronze_df
        # ── Type casts ──────────────────────────────────────────────────
        .withColumn("date",         F.to_date("date", "yyyy-MM-dd"))
        .withColumn("quantity",     F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",   F.col("unit_price").cast(DoubleType()))
        .withColumn("discount_pct", F.coalesce(F.col("discount_pct"), F.lit(0.0)))

        # ── Data quality filters ────────────────────────────────────────
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)

        # ── Derived business columns ────────────────────────────────────
        .withColumn(
            "gross_revenue",
            F.round(F.col("quantity") * F.col("unit_price"), 2)
        )
        .withColumn(
            "discount_amount",
            F.round(F.col("gross_revenue") * F.col("discount_pct"), 2)
        )
        .withColumn(
            "net_revenue",
            F.round(F.col("gross_revenue") - F.col("discount_amount"), 2)
        )

        # ── Calendar dimensions (useful for time-series analysis) ───────
        .withColumn("year",         F.year("date"))
        .withColumn("month",        F.month("date"))
        .withColumn("week",         F.weekofyear("date"))
        .withColumn("day_of_week",  F.dayofweek("date"))   # 1=Sun … 7=Sat
        .withColumn("is_weekend",   F.col("day_of_week").isin(1, 7))

        # ── Silver metadata ─────────────────────────────────────────────
        .withColumn("silver_processed_at", F.current_timestamp())
        .drop("ingestion_timestamp", "source_file", "pipeline_run_date")
    )

    # Upsert into Silver Delta table (idempotent)
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias("existing")
            .merge(
                silver_df.alias("incoming"),
                "existing.transaction_id = incoming.transaction_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("   ✓ Silver table upserted (MERGE)")
    else:
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .save(SILVER_PATH)
        )
        print("   ✓ Silver table created (first run)")

    print(f"   ✓ Silver layer written → {SILVER_PATH}")


# ===========================================================================
# GOLD LAYER — business aggregates consumed by the FastAPI backend
# ===========================================================================
def build_gold_tables() -> None:
    """
    Build four Gold-layer aggregates.  Each is a small, query-optimised
    Delta table that the PostgreSQL loader (or direct JDBC reader) will pick up.
    """
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    print("▶ [GOLD] Building aggregates ...")

    # ── 1. Daily Revenue ────────────────────────────────────────────────────
    daily_revenue = (
        silver_df
        .groupBy("date", "store_region")
        .agg(
            F.sum("net_revenue").alias("total_revenue"),
            F.sum("gross_revenue").alias("gross_revenue"),
            F.sum("discount_amount").alias("total_discounts"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.avg("net_revenue"), 2).alias("avg_basket_value"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy("date", "store_region")
    )
    _write_gold(daily_revenue, GOLD_DAILY, partition_cols=["store_region"])
    print("   ✓ daily_revenue")

    # ── 2. Product Performance ──────────────────────────────────────────────
    product_perf = (
        silver_df
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("net_revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.avg("unit_price"), 2).alias("avg_selling_price"),
            F.round(F.avg("discount_pct") * 100, 2).alias("avg_discount_pct"),
            F.countDistinct("transaction_id").alias("total_transactions"),
            F.countDistinct("store_id").alias("stores_selling"),
        )
        .withColumn(
            "revenue_rank",
            F.dense_rank().over(
                __window().orderBy(F.col("total_revenue").desc())
            )
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy("revenue_rank")
    )
    _write_gold(product_perf, GOLD_PRODUCT)
    print("   ✓ product_performance")

    # ── 3. Store Performance ────────────────────────────────────────────────
    store_perf = (
        silver_df
        .groupBy("store_id", "store_name", "store_region")
        .agg(
            F.sum("net_revenue").alias("total_revenue"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("transaction_id").alias("total_transactions"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.avg("net_revenue"), 2).alias("avg_transaction_value"),
            F.countDistinct("product_id").alias("unique_products_sold"),
        )
        .withColumn(
            "revenue_rank",
            F.dense_rank().over(
                __window().orderBy(F.col("total_revenue").desc())
            )
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy("revenue_rank")
    )
    _write_gold(store_perf, GOLD_STORE)
    print("   ✓ store_performance")

    # ── 4. Category Trends (weekly roll-up) ─────────────────────────────────
    category_trends = (
        silver_df
        .groupBy("year", "week", "category")
        .agg(
            F.sum("net_revenue").alias("weekly_revenue"),
            F.sum("quantity").alias("weekly_units_sold"),
            F.countDistinct("product_id").alias("distinct_products"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy("year", "week", "category")
    )
    _write_gold(category_trends, GOLD_CATEGORY, partition_cols=["year"])
    print("   ✓ category_trends")

    print("▶ [GOLD] All aggregates complete ✓")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def __window():
    """Unpartitioned window spec for global ranking."""
    from pyspark.sql.window import Window
    return Window.orderBy(F.lit(1))   # placeholder — overridden by caller


def _write_gold(df, path: str, partition_cols: list = None) -> None:
    """Overwrite Gold table (full refresh each pipeline run)."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


# ===========================================================================
# PIPELINE ENTRY POINT
# ===========================================================================
def run_pipeline(source_csv: str) -> None:
    print("=" * 60)
    print(" Retail Sales Analytics Pipeline")
    print("=" * 60)
    ingest_to_bronze(source_csv)
    transform_to_silver()
    build_gold_tables()
    print("=" * 60)
    print(" Pipeline completed successfully ✓")
    print("=" * 60)


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "../data/sales_data.csv"
    run_pipeline(csv_path)