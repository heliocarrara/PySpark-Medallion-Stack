
**engineering-project-xlm/**
├── **docker-compose.yml** # Sets up all infra (Postgres, Spark, Jupyter)
├── **.gitignore**
├── **README.md** # Main documentation
│
├── **01_source_simulation/** # STAGE 1: Generation/Simulation
│   ├── **producer.py** # Script that generates prices and transactions
│   └── **requirements.txt**
│
├── **02_ingestion_bronze/** # STAGE 2: Ingestion (Simulating ADF)
│   └── **ingest_to_bronze.py** # Moves from Landing to Bronze
│
├── **03_processing_silver/** # STAGE 3: Cleaning (PySpark)
│   └── **transform_silver.ipynb** # Cleaning notebook and Delta Lake
│
├── **04_analytics_gold/** # STAGE 4: Modeling (SQL)
│   └── **model_gold_tables.sql** # Aggregation queries and KPIs
│
├── **05_dashboard_pbi/** # STAGE 5: Visualization
│   └── **xlm_dashboard.pbix** # Power BI file
│
└── **data_lake/** # LOCAL FOLDER (Simulates Azure Data Lake)
    ├── **landing/** # Where "raw" data first lands
    ├── **bronze/** # Organized original data
    ├── **silver/** # Cleaned data (Delta)
    └── **gold/** # Aggregated data (Ready for PBI)

---
