# 03 - Processing Silver

This stage is responsible for cleaning and refining the raw data from the `bronze` layer. It transforms messy, partitioned JSON files into a structured, high-performance **Delta Lake** table in the `silver` zone.

## Purpose
- Read raw partitioned JSON data from the `bronze` zone.
- Apply a strict schema to ensure data quality.
- Convert string timestamps into Spark `TimestampType`.
- Perform data cleaning:
    - Remove transactions missing mandatory IDs or timestamps.
    - Impute missing prices using the average price of the batch.
- Persist the data using the **Delta Lake** format for reliability (ACID) and performance.

## Components
- `transform_silver.py`: A PySpark script that performs the ETL (Extract, Transform, Load) logic.
- `transform_silver.ipynb`: A Jupyter Notebook version for interactive development and debugging.

## Data Cleaning Logic
1. **Mandatory Fields**: Filter rows where `transaction_id` or `event_ts` is null.
2. **Type Conversion**: Convert `event_ts` (ISO8601 string) to a proper timestamp.
3. **Imputation**: Calculate the average `price_usd` for the current batch and use it to fill any null values in the price column.

## How to use
You can run the processing script inside the provided Spark environment or locally:

```bash
# Run the PySpark transformation
python 03_processing_silver/transform_silver.py
```

### Environment Configuration:
- `LAKE_ROOT`: Optional environment variable to specify the data lake path.
- `SPARK_MASTER`: Spark master URL (defaults to `local[*]`).
- Requires Delta Lake dependencies (automatically handled by the script's Spark config).

## Output
- **Format**: Delta
- **Location**: `data_lake/silver/xlm_transactions/`
