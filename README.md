# Engineering Project XLM

This repository simulates an end-to-end data pipeline (landing → bronze → silver → gold) using Docker for local infrastructure.

## Architecture
- Postgres: simulates user registry/reference data.
- Spark (master + worker): distributed data processing.
- JupyterLab (PySpark): notebook-based development and exploration.
- data_lake/: local folder acting as Azure Data Lake Gen2.

Diagram: create in Excalidraw and export as an image. Suggestion: https://excalidraw.com/

## How to Run
1. Install and open Docker Desktop.
2. From the project root, run:
   - `docker compose up -d`
3. Verify:
   - Spark Master UI: http://localhost:8080
   - Spark Worker UI: http://localhost:8081
   - JupyterLab: http://localhost:8888
   - Postgres: localhost:5432

To stop:
- `docker compose down`

To restart:
- `docker compose down && docker compose up -d`

## Stack & Connections
- Spark Master: `spark://spark-master:7077` inside the Docker network.
- Jupyter maps notebooks from [03_processing_silver](file:///c:/Users/helio/source/repos/engineering-project-xlm/03_processing_silver) and the lake from [data_lake](file:///c:/Users/helio/source/repos/engineering-project-xlm/data_lake).
- Compose file: [docker-compose.yml](file:///c:/Users/helio/source/repos/engineering-project-xlm/docker-compose.yml)

Notebook initialization example:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("engineering-project-xlm")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
```

## Folder Structure
See the folder map: [folders-map.md](file:///c:/Users/helio/source/repos/engineering-project-xlm/folders-map.md)

