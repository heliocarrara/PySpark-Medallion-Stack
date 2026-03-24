# 04 - Analytics Gold

This stage is the final layer of our data pipeline. it consumes the cleaned data from the `silver` zone and joins it with reference data to create high-value, aggregated tables ready for BI tools and dashboards.

## Purpose
- Read cleaned transaction data from the **Silver** layer.
- Enrich transaction data by joining it with user reference data (from an external Postgres database).
- Calculate business-ready aggregations:
    - **Hourly**: Performance metrics sliced by hour.
    - **Daily**: Daily trends of price, volume, and count.
    - **By Country**: Regional analysis combining transaction value and user location.
- Persist the results as **Delta Lake** tables in the `gold` zone for consumption.

## Components
- `transform_gold.py`: A PySpark script that performs the enrichment and aggregation logic.
- `model_gold_tables.sql`: SQL definitions for the gold layer views or tables.
- `transform_gold.ipynb`: Interactive notebook for exploring dependencies and join logic.

## Enrichments & Joins
The script performs a JDBC connection to a Postgres instance (`jdbc:postgresql://postgres:5432/xlm`) to fetch the `users` table. It then performs an **Inner Join** between transactions and users based on `user_id` to enable geographic analysis.

## Aggregated Tables
| Table | Partition/Key | Metrics |
|---|---|---|
| `xlm_hourly` | `hour_ts` | avg_price, total_volume, total_notional, tx_count |
| `xlm_daily` | `day_ts` | avg_price, total_volume, total_notional, tx_count |
| `xlm_by_country_daily` | `day_ts`, `country_code` | avg_price, total_volume, total_notional, tx_count |

## How to use
Run the analytics script inside the Spark environment:

```bash
# Run the PySpark analytics transformation
python 04_analytics_gold/transform_gold.py
```

### Requirements:
- Access to the `silver` Delta table.
- Network connectivity to the `postgres` container.
- PostgreSQL JDBC driver (configured in the script).

## Output
- **Delta**: `data_lake/gold/{xlm_hourly, xlm_daily, xlm_by_country_daily}`
- **Parquet (for Streamlit)**: `data_lake/gold/parquet/{xlm_hourly, xlm_daily, xlm_by_country_daily}`
