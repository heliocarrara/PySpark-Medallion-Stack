# 05 - Dashboard Streamlit (Python)

Este projeto agora usa **Streamlit** para visualização. O app lê os dados da camada Gold em **Parquet** direto do lake.

## Como rodar
1. Suba a stack:
   - `docker compose up -d`
2. Gere os dados da camada Gold:
   - `python 04_analytics_gold/transform_gold.py`
3. Abra o dashboard:
   - http://localhost:8501

## Onde o app lê os dados
- `data_lake/gold/parquet/xlm_hourly`
- `data_lake/gold/parquet/xlm_daily`
- `data_lake/gold/parquet/xlm_by_country_daily`
