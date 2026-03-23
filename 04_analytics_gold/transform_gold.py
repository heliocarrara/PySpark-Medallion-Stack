from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def find_lake_root(start: Path) -> Path:
    env_value = os.environ.get("LAKE_ROOT")
    if env_value:
        candidate = Path(env_value).expanduser().resolve()
        if candidate.exists():
            return candidate

    jovyan_candidate = Path("/home/jovyan/work/data_lake")
    if jovyan_candidate.exists():
        return jovyan_candidate

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
    spark_master = os.environ.get("SPARK_MASTER", "local[*]")
    return (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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


def main() -> int:
    lake_root = find_lake_root(Path.cwd())

    silver_path = str(lake_root / "silver" / "xlm_transactions")
    gold_root = lake_root / "gold"
    gold_hourly_path = str(gold_root / "xlm_hourly")
    gold_daily_path = str(gold_root / "xlm_daily")
    gold_country_daily_path = str(gold_root / "xlm_by_country_daily")

    spark = build_spark_session("engineering-project-xlm-gold")

    silver_df = spark.read.format("delta").load(silver_path)

    jdbc_url = "jdbc:postgresql://postgres:5432/xlm"
    jdbc_props = {
        "user": "xlm",
        "password": "xlm",
        "driver": "org.postgresql.Driver",
    }
    users_df = spark.read.jdbc(url=jdbc_url, table="users", properties=jdbc_props)

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

    gold_root.mkdir(parents=True, exist_ok=True)
    hourly_df.write.mode("overwrite").parquet(gold_hourly_path)
    daily_df.write.mode("overwrite").parquet(gold_daily_path)
    by_country_daily_df.write.mode("overwrite").parquet(gold_country_daily_path)

    print(f"WROTE {gold_hourly_path}")
    print(f"WROTE {gold_daily_path}")
    print(f"WROTE {gold_country_daily_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
