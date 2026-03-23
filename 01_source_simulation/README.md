# 01 - Source Simulation

This stage is responsible for generating simulated transaction data for the XLM asset. It acts as the "Producer" in our data pipeline, creating raw JSON files that are placed in the `data_lake/landing` directory.

## Purpose
- Simulate real-time or batch transaction events.
- Generate valid JSON objects following the `XlmTransactionV1` schema.
- Populate the initial "Landing" zone of the data lake.

## Components
- `producer.py`: A Python script that uses `argparse` to generate a specified number of transaction files.

## Data Schema (XlmTransactionV1)
| Field | Type | Description |
|---|---|---|
| `transaction_id` | `str` (UUID) | Unique identifier for each transaction. |
| `user_id` | `int` | Random identifier for a user (simulating reference data). |
| `asset_symbol` | `str` | Always "XLM" in this simulation. |
| `price_usd` | `float` | Current price of XLM in USD. |
| `volume_xlm` | `float` | Amount of XLM transacted. |
| `notional_usd` | `float` | Total value (Price * Volume). |
| `event_ts` | `str` (ISO8601) | Timestamp of the transaction event. |
| `schema_version`| `str` | Version indicator (default: "v1"). |

## How to use
Run the producer from the project root or from within this directory:

```bash
# Generate 10 transactions in the default landing directory
python 01_source_simulation/producer.py --count 10

# Generate transactions with specific ranges
python 01_source_simulation/producer.py --count 5 --min-price 0.12 --max-price 0.14
```

### Script Arguments:
- `--count`: Number of JSON files to generate (default: 1).
- `--output-dir`: Where to save the files (default: `../data_lake/landing`).
- `--seed`: For reproducible random generation.
- `--min-price` / `--max-price`: Range for XLM price simulation.
- `--min-volume` / `--max-volume`: Range for volume simulation.
- `--user-id-min` / `--user-id-max`: Range for user IDs.
