# Docker Compose Runbook (Local Dev)

This repository ships a local infrastructure stack using Docker Compose so you can run the project end-to-end on your machine:

- Postgres (data source simulation)
- Spark (master + worker) for distributed processing
- JupyterLab (PySpark-ready) for notebooks and exploration
- Streamlit (Python dashboard)

## Prerequisites

- Docker Desktop installed and running
- Docker Compose v2 (bundled with Docker Desktop)

Quick check:

```bash
docker --version
docker compose version
```

## Start the stack

From the repository root (where `docker-compose.yml` lives):

```bash
docker compose up -d
```

Verify containers:

```bash
docker compose ps
```

## Stop the stack

Stops containers but keeps volumes/data:

```bash
docker compose down
```

## Restart the stack (containers)

The simplest “restart everything” flow:

```bash
docker compose down
docker compose up -d
```

Restart a single service:

```bash
docker compose restart jupyter
docker compose restart postgres
docker compose restart spark-master
docker compose restart spark-worker-1
```

## Reset everything (including Postgres data)

This removes containers and volumes. Use when you want a fresh database.

```bash
docker compose down -v
```

Also delete the local folder that persists Postgres data:

- `./postgres-data/`

## If Docker Desktop is “stuck”: restart Docker itself

Sometimes the Docker daemon/desktop gets into a bad state. Use one of the options below.

### Option A: Restart Docker Desktop via UI (recommended)

1. Quit Docker Desktop (system tray icon → Quit)
2. Open Docker Desktop again
3. Wait until it shows “Docker is running”
4. Start the stack again:

```bash
docker compose up -d
```

### Option B: Restart Docker service (Windows, Admin)

Run PowerShell as Administrator:

```powershell
Restart-Service com.docker.service
```

Then start the stack again:

```bash
docker compose up -d
```

## URLs (local)

- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- JupyterLab: http://localhost:8888
- Streamlit: http://localhost:8501
- Postgres: localhost:5432

## Logs and debugging

Follow logs for all services:

```bash
docker compose logs -f
```

Logs for one service:

```bash
docker compose logs -f jupyter
docker compose logs -f streamlit
docker compose logs -f spark-master
docker compose logs -f spark-worker-1
docker compose logs -f postgres
```

Re-pull images (useful after changing tags):

```bash
docker compose pull
```

Check the fully-rendered compose config:

```bash
docker compose config
```

## Jupyter: connect to the Spark cluster

The Spark master address inside the Docker network is:

- `spark://spark-master:7077`

Example snippet for notebooks:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("engineering-project-xlm")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
```
