import argparse
import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class XlmTransactionV1:
    transaction_id: str
    user_id: int
    asset_symbol: str
    price_usd: float
    volume_xlm: float
    notional_usd: float
    event_ts: str
    schema_version: str = "v1"


def _format_event_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _filename_timestamp(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def generate_transaction(
    *,
    rng: random.Random,
    event_dt: datetime,
    min_price: float,
    max_price: float,
    min_volume: float,
    max_volume: float,
    user_id_min: int,
    user_id_max: int,
) -> XlmTransactionV1:
    price = round(rng.uniform(min_price, max_price), 5)
    volume = round(rng.uniform(min_volume, max_volume), 2)
    notional = round(price * volume, 2)

    return XlmTransactionV1(
        transaction_id=str(uuid.uuid4()),
        user_id=rng.randint(user_id_min, user_id_max),
        asset_symbol="XLM",
        price_usd=price,
        volume_xlm=volume,
        notional_usd=notional,
        event_ts=_format_event_ts(event_dt),
    )


def write_transaction_json(*, output_dir: Path, tx: XlmTransactionV1, event_dt: datetime) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"xlm_{tx.schema_version}_{_filename_timestamp(event_dt)}.json"
    output_path = output_dir / file_name

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(asdict(tx), f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate fake XLM transaction JSON files into data_lake/landing.")
    parser.add_argument("--count", type=int, default=1, help="Number of JSON files to generate.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: <repo>/data_lake/landing).",
    )
    parser.add_argument("--seed", type=int, default=None, help="Optional RNG seed for reproducible outputs.")
    parser.add_argument("--min-price", type=float, default=0.10, help="Minimum XLM price in USD.")
    parser.add_argument("--max-price", type=float, default=0.15, help="Maximum XLM price in USD.")
    parser.add_argument("--min-volume", type=float, default=10.0, help="Minimum transaction volume in XLM.")
    parser.add_argument("--max-volume", type=float, default=5000.0, help="Maximum transaction volume in XLM.")
    parser.add_argument("--user-id-min", type=int, default=1, help="Minimum user_id value.")
    parser.add_argument("--user-id-max", type=int, default=1000, help="Maximum user_id value.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1:
        raise SystemExit("--count must be >= 1")

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    output_dir: Path = args.output_dir if args.output_dir is not None else (repo_root / "data_lake" / "landing")

    rng = random.Random(args.seed)
    base_dt = datetime.now(timezone.utc)

    written: list[Path] = []
    for i in range(args.count):
        event_dt = base_dt + timedelta(microseconds=i)
        tx = generate_transaction(
            rng=rng,
            event_dt=event_dt,
            min_price=args.min_price,
            max_price=args.max_price,
            min_volume=args.min_volume,
            max_volume=args.max_volume,
            user_id_min=args.user_id_min,
            user_id_max=args.user_id_max,
        )
        written.append(write_transaction_json(output_dir=output_dir, tx=tx, event_dt=event_dt))

    for path in written:
        print(path.as_posix())

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
