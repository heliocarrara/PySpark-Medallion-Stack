# 05 - Streamlit Dashboard (Python)

This project uses **Streamlit** for visualization. The app reads Gold-layer data in **Parquet** directly from the lake.

## How to run
1. Start the stack:
   - `docker compose up -d`
2. Generate Gold-layer data:
   - `python 04_analytics_gold/transform_gold.py`
3. Open the dashboard:
   - http://localhost:8501

## Where the app reads data from
- `data_lake/gold/parquet/xlm_hourly`
- `data_lake/gold/parquet/xlm_daily`
- `data_lake/gold/parquet/xlm_by_country_daily`
