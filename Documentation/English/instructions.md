
---

### 1. Project Root (`/`)
* **`docker-compose.yml`**: Defines the local infrastructure. It should start containers for **Spark**, **Postgres** (to simulate user registry/reference data), and **JupyterLab** (so you can code notebooks from VS Code via browser or extension).
* **`README.md`**: Your project overview. Include an architecture diagram (you can draw it in [Excalidraw](https://excalidraw.com/)), explain the stack, and provide instructions to run `docker compose`.
* **`.gitignore`**: Essential to avoid committing junk, `__pycache__` folders, or heavy data from `data_lake/` (commit only the folder structure, not thousands of JSONs).
* Status: Infra iniciada com Docker Compose (xlm-postgres, xlm-spark-master, xlm-spark-worker-1, xlm-jupyter, xlm-streamlit em execução).

---

### 2. `01_source_simulation/` (The Source)
Simulate the system that generates money/data for the company.
* **`producer.py`**: Script that generates an XLM transaction JSON. To simulate the API, use Python’s `random` library to fluctuate the price between **0.10 and 0.15** and produce random volumes.
* **Business Logic**: The script must save the file into `data_lake/landing/`. Use a realistic filename: `xlm_v1_TIMESTAMP.json`.
* Status: 10 arquivos gerados em `data_lake/landing/` com preços entre 0.10–0.15 e volumes aleatórios.

---

### 3. `02_ingestion_bronze/` (The “Fake” Data Factory)
Azure Data Factory (ADF) moves data. Show you understand **metadata**.
* **`ingest_to_bronze.py`**: Reads from `landing/` and moves to `bronze/`.
* **The Differentiator**: While moving data, the script must create date-partitioned folders: `/bronze/year=2026/month=03/day=23/`. This mimics what ADF does under the hood to optimize performance.
* Status: 10 arquivos movidos para `bronze/year=2026/month=03/day=23/`.

---

### 4. `03_processing_silver/` (The PySpark Kingdom)
Here you shine with solid engineering code.
* **`transform_silver.ipynb`**: Notebook that reads Bronze JSONs using **PySpark**.
* **What to do here**:
    * Define the `Schema` (don’t let Spark infer; set explicit types).
    * Handle nulls (if price is null, drop the row or fill with the mean).
    * **Delta Lake**: Save the final result in `.delta` format. Delta supports “UPDATE” and “DELETE” on lake data—an important Databricks requirement.
* Status: Silver escrito em Delta (`silver/xlm_transactions`) com 20 linhas processadas.

---

### 5. `04_analytics_gold/` (SQL Modeling)
Prepare the banquet for business analysts.
* **`model_gold_tables.sql`**: Queries that take Silver tables and transform them into business tables.
* **What to do here**:
    * **Aggregations**: Average price per hour, total volume per day.
    * **Join**: Join transactions with the users table (in your Docker Postgres) to know which country the purchase came from.
    * Save the final result in the `gold/` folder (Delta or Parquet).
* Status: Gold gerado em Delta (`gold/xlm_hourly`, `gold/xlm_daily`, `gold/xlm_by_country_daily`) com leitura de `users` via Postgres.

---

### 6. `05_dashboard_python/` (Visualization)
* **`app.py`**: Streamlit app que lê as tabelas da camada Gold e renderiza gráficos.
* **Como consome dados**: lê os Parquets gerados em `data_lake/gold/parquet/` (`xlm_hourly`, `xlm_daily`, `xlm_by_country_daily`).
* **Como rodar**: subir o serviço `streamlit` no `docker-compose.yml` e abrir http://localhost:8501.
* Status: Dashboard Streamlit pronto (porta 8501) lendo Parquet da camada Gold.

---

### 7. `data_lake/` (Storage)
This folder simulates your **Azure Data Lake Gen2**. Organize it so any tool can find what it needs.
* **Landing**: Raw, unorganized data.
* **Bronze**: Original data, partitioned by date.
* **Silver**: Cleaned, typed data in Delta format.
* **Gold**: BI-ready tables (Star Schema).
* Status: Estrutura pronta — `landing/` populada, `bronze/` particionada por data, `silver/xlm_transactions` em Delta, `gold/` com tabelas Delta para BI.

---
