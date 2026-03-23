# 02 - Ingestion to Bronze

This stage is responsible for moving raw data from the `landing` zone to the `bronze` zone. It performs the first step of organizing the data lake by applying partitioning.

## Purpose
- Move data from `landing` to `bronze` (ingestion).
- Organize files into a temporal partition structure: `year/month/day`.
- Ensure files are moved safely (handling name conflicts).
- Clean up the `landing` zone as files are processed.

## Components
- `ingest_to_bronze.py`: A Python script that scans the landing directory, parses file metadata, and moves files to their corresponding partitions.

## Partitioning Strategy
Files are moved to directories following this structure:
`data_lake/bronze/year=YYYY/month=MM/day=DD/`

The date is determined by:
1. The `event_ts` field inside the JSON payload.
2. If the field is missing or invalid, the file's last modification time (`mtime`) is used.

## Conflict Resolution
If a file with the same name already exists in the destination partition, the script appends a unique UUID hex string to the filename to prevent overwriting data.

## How to use
Run the ingestion script from the project root or from within this directory:

```bash
# Ingest all XLM v1 files from landing to bronze
python 02_ingestion_bronze/ingest_to_bronze.py

# Perform a dry run to see what files would be moved
python 02_ingestion_bronze/ingest_to_bronze.py --dry-run

# Limit the number of files to ingest
python 02_ingestion_bronze/ingest_to_bronze.py --max-files 50
```

### Script Arguments:
- `--landing-dir`: Source directory (default: `../data_lake/landing`).
- `--bronze-dir`: Destination directory (default: `../data_lake/bronze`).
- `--pattern`: Filename glob pattern (default: `xlm_v1_*.json`).
- `--max-files`: Limit the number of files processed in one run.
- `--dry-run`: Preview movements without performing them.
