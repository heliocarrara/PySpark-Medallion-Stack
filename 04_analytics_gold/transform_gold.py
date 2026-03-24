"""
Gold Layer Transformation: Silver to Gold.

This script aggregates cleaned transaction data from the 'silver' zone
and enriches it with user reference data from an external Postgres database.
It generates hourly and daily aggregated tables for final analytics.

Usage:
    python transform_gold.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    Initializes a Spark Session with Delta Lake and PostgreSQL JDBC support.
    """
    spark_master = os.environ.get("SPARK_MASTER", "local[*]")
    return (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "io.delta:delta-spark_2.12:3.2.0",
                    "org.postgresql:postgresql:42.7.3",
                ]
            ),
        )
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
    Main Gold-layer logic to enrich and aggregate silver data.
    """
    # 1. Setup paths
    lake_root = find_lake_root(Path.cwd())
    silver_path = str(lake_root / "silver" / "xlm_transactions")
    silver_prices_path = str(lake_root / "silver" / "xlm_price_ticks")
    gold_root = lake_root / "gold"
    gold_hourly_path = str(gold_root / "xlm_hourly")
    gold_daily_path = str(gold_root / "xlm_daily")
    gold_country_daily_path = str(gold_root / "xlm_by_country_daily")
    gold_price_hourly_path = str(gold_root / "xlm_price_hourly_by_exchange")
    gold_price_daily_path = str(gold_root / "xlm_price_daily_by_exchange")
    parquet_export_root = gold_root / "parquet"
    parquet_hourly_path = str(parquet_export_root / "xlm_hourly")
    parquet_daily_path = str(parquet_export_root / "xlm_daily")
    parquet_country_daily_path = str(parquet_export_root / "xlm_by_country_daily")
    parquet_price_hourly_path = str(parquet_export_root / "xlm_price_hourly_by_exchange")
    parquet_price_daily_path = str(parquet_export_root / "xlm_price_daily_by_exchange")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    parquet_hourly_run_path = str(parquet_export_root / f"xlm_hourly__{run_id}")
    parquet_daily_run_path = str(parquet_export_root / f"xlm_daily__{run_id}")
    parquet_country_daily_run_path = str(parquet_export_root / f"xlm_by_country_daily__{run_id}")
    parquet_price_hourly_run_path = str(parquet_export_root / f"xlm_price_hourly_by_exchange__{run_id}")
    parquet_price_daily_run_path = str(parquet_export_root / f"xlm_price_daily_by_exchange__{run_id}")

    # 2. Build Spark Session
    spark = build_spark_session("engineering-project-xlm-gold")

    # 3. Read Silver Data (Delta)
    silver_df = spark.read.format("delta").load(silver_path)
    price_silver_exists = Path(silver_prices_path).exists()
    prices_df = spark.read.format("delta").load(silver_prices_path) if price_silver_exists else None

    # 4. Fetch User Data (PostgreSQL via JDBC)
    jdbc_url = "jdbc:postgresql://postgres:5432/xlm"
    jdbc_props = {
        "user": "xlm",
        "password": "xlm",
        "driver": "org.postgresql.Driver",
    }
    users_df = spark.read.jdbc(url=jdbc_url, table="users", properties=jdbc_props)

    # 5. Aggregate: Hourly Performance
    hourly_df = (
        silver_df.groupBy(F.date_trunc("hour", F.col("event_ts")).alias("hour_ts"))
        .agg(
            F.avg("price_usd").alias("avg_price_usd"),
            F.sum("volume_xlm").alias("total_volume_xlm"),
            F.sum("notional_usd").alias("total_notional_usd"),
            F.count(F.lit(1)).alias("tx_count"),
        )
        .orderBy(F.col("hour_ts").asc())
    )

    # 6. Aggregate: Daily Trends
    daily_df = (
        silver_df.groupBy(F.date_trunc("day", F.col("event_ts")).alias("day_ts"))
        .agg(
            F.avg("price_usd").alias("avg_price_usd"),
            F.sum("volume_xlm").alias("total_volume_xlm"),
            F.sum("notional_usd").alias("total_notional_usd"),
            F.count(F.lit(1)).alias("tx_count"),
        )
        .orderBy(F.col("day_ts").asc())
    )

    # 7. Aggregate: Daily Trends By Country (Join included)
    by_country_daily_df = (
        silver_df.alias("s")
        .join(users_df.alias("u"), F.col("s.user_id") == F.col("u.user_id"), "inner")
        .groupBy(
            F.date_trunc("day", F.col("s.event_ts")).alias("day_ts"),
            F.col("u.country_code"),
        )
        .agg(
            F.avg(F.col("s.price_usd")).alias("avg_price_usd"),
            F.sum(F.col("s.volume_xlm")).alias("total_volume_xlm"),
            F.sum(F.col("s.notional_usd")).alias("total_notional_usd"),
            F.count(F.lit(1)).alias("tx_count"),
        )
        .orderBy(F.col("day_ts").asc(), F.col("country_code").asc())
    )

    # 8. Ensure storage and write all Gold tables (Delta)
    ensure_writable_dir(gold_root)
    ensure_writable_dir(Path(gold_hourly_path))
    ensure_writable_dir(Path(gold_daily_path))
    ensure_writable_dir(Path(gold_country_daily_path))
    ensure_writable_dir(Path(gold_price_hourly_path))
    ensure_writable_dir(Path(gold_price_daily_path))
    ensure_writable_dir(parquet_export_root)

    hourly_df.write.format("delta").mode("overwrite").save(gold_hourly_path)
    daily_df.write.format("delta").mode("overwrite").save(gold_daily_path)
    by_country_daily_df.write.format("delta").mode("overwrite").save(gold_country_daily_path)

    hourly_df.write.mode("overwrite").parquet(parquet_hourly_run_path)
    daily_df.write.mode("overwrite").parquet(parquet_daily_run_path)
    by_country_daily_df.write.mode("overwrite").parquet(parquet_country_daily_run_path)

    if prices_df is not None:
        price_hourly_by_exchange_df = (
            prices_df.groupBy(
                F.date_trunc("hour", F.col("event_ts")).alias("hour_ts"),
                F.col("exchange"),
                F.col("quote_symbol"),
            )
            .agg(
                F.avg("last_price").alias("avg_price"),
                F.min("last_price").alias("min_price"),
                F.max("last_price").alias("max_price"),
                F.count(F.lit(1)).alias("tick_count"),
            )
            .orderBy(F.col("hour_ts").asc(), F.col("exchange").asc())
        )
        price_daily_by_exchange_df = (
            prices_df.groupBy(
                F.date_trunc("day", F.col("event_ts")).alias("day_ts"),
                F.col("exchange"),
                F.col("quote_symbol"),
            )
            .agg(
                F.avg("last_price").alias("avg_price"),
                F.min("last_price").alias("min_price"),
                F.max("last_price").alias("max_price"),
                F.count(F.lit(1)).alias("tick_count"),
            )
            .orderBy(F.col("day_ts").asc(), F.col("exchange").asc())
        )

        price_hourly_by_exchange_df.write.format("delta").mode("overwrite").save(gold_price_hourly_path)
        price_daily_by_exchange_df.write.format("delta").mode("overwrite").save(gold_price_daily_path)
        price_hourly_by_exchange_df.write.mode("overwrite").parquet(parquet_price_hourly_run_path)
        price_daily_by_exchange_df.write.mode("overwrite").parquet(parquet_price_daily_run_path)

    # 10. Results
    print(f"WROTE Delta: {gold_hourly_path}")
    print(f"WROTE Delta: {gold_daily_path}")
    print(f"WROTE Delta: {gold_country_daily_path}")
    if prices_df is not None:
        print(f"WROTE Delta: {gold_price_hourly_path}")
        print(f"WROTE Delta: {gold_price_daily_path}")
    print(f"WROTE Parquet: {parquet_hourly_run_path}")
    print(f"WROTE Parquet: {parquet_daily_run_path}")
    print(f"WROTE Parquet: {parquet_country_daily_run_path}")
    if prices_df is not None:
        print(f"WROTE Parquet: {parquet_price_hourly_run_path}")
        print(f"WROTE Parquet: {parquet_price_daily_run_path}")
    print("FINISHED.")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
