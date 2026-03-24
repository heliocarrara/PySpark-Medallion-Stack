"""
Silver Layer Transformation: Bronze to Silver.

This script processes raw JSON data from the 'bronze' zone, cleans it,
and saves it as a Delta Lake table in the 'silver' zone.
It handles schema enforcement, timestamp conversion, and basic data imputation.

Usage:
    python transform_silver.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def find_lake_root(start: Path) -> Path:
    """
    Attempts to locate the 'data_lake' directory by searching upwards
    from the start directory or using environment variables.
    """
    env_value = os.environ.get("LAKE_ROOT")
    if env_value:
        candidate = Path(env_value).expanduser().resolve()
        if candidate.exists():
            return candidate

    # Check common Docker-based paths
    jovyan_candidate = Path("/home/jovyan/work/data_lake")
    if jovyan_candidate.exists():
        return jovyan_candidate

    # Recursive search upwards
    current = start.resolve()
    for _ in range(12):
        candidate = current / "data_lake"
        if candidate.exists():
            return candidate
        if current.parent == current:
            break
        current = current.parent
    raise RuntimeError("Could not find data_lake folder")


def build_spark_session(app_name: str) -> SparkSession:
    """
    Initializes a Spark Session with Delta Lake extension and configuration.
    """
    spark_master = os.environ.get("SPARK_MASTER", "local[*]")
    return (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .getOrCreate()
    )


def ensure_writable_dir(path: Path) -> None:
    """
    Ensures a directory exists and attempts to set broad write permissions.
    """
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o777)
    except PermissionError:
        return


def main() -> int:
    """
    Main ETL logic to read from bronze, clean, and write to silver.
    """
    # 1. Setup paths
    lake_root = find_lake_root(Path.cwd())
    bronze_path = str(lake_root / "bronze")
    silver_base_dir = lake_root / "silver"
    silver_dir = silver_base_dir / "xlm_transactions"
    silver_prices_dir = silver_base_dir / "xlm_price_ticks"
    silver_parquet_dir = silver_base_dir / "parquet"
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    silver_prices_parquet_run = silver_parquet_dir / f"xlm_price_ticks__{run_id}"

    ensure_writable_dir(silver_base_dir)
    ensure_writable_dir(silver_dir)
    ensure_writable_dir(silver_prices_dir)
    ensure_writable_dir(silver_parquet_dir)
    silver_path = str(silver_dir)
    silver_prices_path = str(silver_prices_dir)
    silver_prices_parquet_run_path = str(silver_prices_parquet_run)

    # 2. Build Spark Session
    spark = build_spark_session("engineering-project-xlm-silver")

    # 3. Define schema for raw data
    schema = StructType(
        [
            StructField("transaction_id", StringType(), nullable=False),
            StructField("user_id", IntegerType(), nullable=True),
            StructField("asset_symbol", StringType(), nullable=True),
            StructField("price_usd", DoubleType(), nullable=True),
            StructField("volume_xlm", DoubleType(), nullable=True),
            StructField("notional_usd", DoubleType(), nullable=True),
            StructField("event_ts", StringType(), nullable=True),
            StructField("schema_version", StringType(), nullable=True),
        ]
    )

    # 4. Read raw JSON data
    raw_df = spark.read.schema(schema).option("recursiveFileLookup", "true").json(bronze_path)
    raw_any_df = spark.read.option("recursiveFileLookup", "true").json(bronze_path)

    # 5. Transformations: cast timestamp and impute
    df = raw_df.withColumn(
        "event_ts",
        F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
    )

    # Calculate average price for imputation
    avg_price_row = df.select(F.avg("price_usd").alias("avg_price")).first()
    avg_price = avg_price_row["avg_price"] if avg_price_row is not None else None
    fill_value = float(avg_price) if avg_price is not None else None

    # Apply filters and imputation
    clean_df = (
        df.filter(F.col("transaction_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .withColumn(
            "price_usd",
            F.when(F.col("price_usd").isNull(), F.lit(fill_value)).otherwise(F.col("price_usd")),
        )
        .filter(F.col("price_usd").isNotNull())
    )

    api_df = (
        raw_any_df.filter(F.col("schema_version") == F.lit("api_v1"))
        .withColumn(
            "event_ts",
            F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        )
        .select(
            F.col("asset_symbol").cast("string").alias("asset_symbol"),
            F.col("exchange").cast("string").alias("exchange"),
            F.col("url").cast("string").alias("url"),
            F.col("event_ts").alias("event_ts"),
            F.col("parsed.last_price").cast("double").alias("last_price"),
            F.col("parsed.quote_symbol").cast("string").alias("quote_symbol"),
            F.col("parsed.parse_error").cast("string").alias("parse_error"),
            F.col("schema_version").cast("string").alias("schema_version"),
        )
    )

    clean_api_df = (
        api_df.filter(F.col("event_ts").isNotNull())
        .filter(F.col("exchange").isNotNull())
        .filter(F.col("last_price").isNotNull())
        .filter(F.col("parse_error").isNull())
    )

    # 6. Write to Silver (Delta)
    clean_df.write.format("delta").mode("overwrite").save(silver_path)
    clean_api_df.write.format("delta").mode("overwrite").save(silver_prices_path)
    clean_api_df.write.mode("overwrite").parquet(silver_prices_parquet_run_path)

    # 7. Finalize
    print(f"WROTE {silver_path}")
    print(f"ROWS {clean_df.count()}")
    print(f"WROTE {silver_prices_path}")
    print(f"ROWS {clean_api_df.count()}")
    print(f"WROTE {silver_prices_parquet_run_path}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
