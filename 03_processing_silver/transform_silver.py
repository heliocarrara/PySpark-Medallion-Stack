from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


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
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )


def ensure_writable_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o777)
    except PermissionError:
        return



def main() -> int:
    lake_root = find_lake_root(Path.cwd())
    bronze_path = str(lake_root / "bronze")
    silver_base_dir = lake_root / "silver"
    silver_dir = silver_base_dir / "xlm_transactions"
    ensure_writable_dir(silver_base_dir)
    ensure_writable_dir(silver_dir)
    silver_path = str(silver_dir)

    spark = build_spark_session("engineering-project-xlm-silver")

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

    raw_df = spark.read.schema(schema).option("recursiveFileLookup", "true").json(bronze_path)

    df = raw_df.withColumn(
        "event_ts",
        F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
    )

    avg_price_row = df.select(F.avg("price_usd").alias("avg_price")).first()
    avg_price = avg_price_row["avg_price"] if avg_price_row is not None else None

    fill_value = float(avg_price) if avg_price is not None else None

    clean_df = (
        df.filter(F.col("transaction_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
        .withColumn(
            "price_usd",
            F.when(F.col("price_usd").isNull(), F.lit(fill_value)).otherwise(F.col("price_usd")),
        )
        .filter(F.col("price_usd").isNotNull())
    )

    clean_df.write.format("delta").mode("overwrite").save(silver_path)

    print(f"WROTE {silver_path}")
    print(f"ROWS {clean_df.count()}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
