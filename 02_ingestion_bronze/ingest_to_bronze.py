"""
Ingestion Script: Landing to Bronze.

This script moves raw JSON files from the 'landing' directory to a partitioned
'bronze' directory. It uses the file content or modification times to determine
the correct destination partition (year/month/day).

Usage:
    python ingest_to_bronze.py --max-files 100 --dry-run
"""

import argparse
import json
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class IngestResult:
    """
    Summary of the ingestion execution results.
    """
    scanned: int
    moved: int
    skipped: int


def _parse_event_ts(value: object) -> datetime | None:
    """
    Parses an ISO8601 string or 'Z' formatted timestamp into a UTC datetime object.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _safe_destination_path(dest_dir: Path, filename: str) -> Path:
    """
    Generates a unique destination path if the filename already exists in the target directory.
    """
    candidate = dest_dir / filename
    if not candidate.exists():
        return candidate
    # Append unique ID if file already exists to avoid overwriting
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    return dest_dir / f"{stem}_{uuid.uuid4().hex}{suffix}"


def ingest_one(*, source_path: Path, bronze_root: Path, dry_run: bool) -> bool:
    """
    Processes a single file from landing to the appropriate bronze partition.
    """
    try:
        # Attempt to read file content to find event_ts
        payload = json.loads(source_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        # Skip if file is unreadable or not a valid JSON
        return False

    # Extract date from payload or fallback to file modification time
    event_dt = _parse_event_ts(payload.get("event_ts") if isinstance(payload, dict) else None)
    if event_dt is None:
        try:
            event_dt = datetime.fromtimestamp(source_path.stat().st_mtime, tz=timezone.utc)
        except OSError:
            return False

    # Define Hive-style partition directory: year=YYYY/month=MM/day=DD
    dest_dir = (
        bronze_root
        / f"year={event_dt.year:04d}"
        / f"month={event_dt.month:02d}"
        / f"day={event_dt.day:02d}"
    )
    dest_path = _safe_destination_path(dest_dir, source_path.name)

    if dry_run:
        print(f"DRY_RUN {source_path.as_posix()} -> {dest_path.as_posix()}")
        return True

    # Ensure partition folder exists and move the file
    dest_dir.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_path), str(dest_path))
    print(f"MOVED {source_path.as_posix()} -> {dest_path.as_posix()}")
    return True


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the ingestion script.
    """
    parser = argparse.ArgumentParser(description="Move JSON files from data_lake/landing to data_lake/bronze (partitioned).")
    parser.add_argument(
        "--landing-dir",
        type=Path,
        default=None,
        help="Landing directory (default: <repo>/data_lake/landing).",
    )
    parser.add_argument(
        "--bronze-dir",
        type=Path,
        default=None,
        help="Bronze directory (default: <repo>/data_lake/bronze).",
    )
    parser.add_argument("--pattern", type=str, default="xlm_*.json", help="Filename glob pattern to ingest.")
    parser.add_argument("--max-files", type=int, default=None, help="Optional limit of files to ingest.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without moving files.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="If set, runs continuously, ingesting every N seconds.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Optional number of iterations when running with --interval-seconds. If omitted, runs forever.",
    )
    return parser.parse_args()


def main() -> int:
    """
    Main loop to find and process files from landing to bronze.
    """
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    # Setup directories
    landing_dir: Path = args.landing_dir if args.landing_dir is not None else (repo_root / "data_lake" / "landing")
    bronze_dir: Path = args.bronze_dir if args.bronze_dir is not None else (repo_root / "data_lake" / "bronze")

    if not landing_dir.exists():
        raise SystemExit(f"Landing directory does not exist: {landing_dir.as_posix()}")

    if args.max_files is not None and args.max_files < 1:
        raise SystemExit("--max-files must be >= 1")
    if args.interval_seconds is not None and args.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be > 0")
    if args.iterations is not None and args.iterations < 1:
        raise SystemExit("--iterations must be >= 1")

    iteration = 0
    while True:
        iteration += 1
        iteration_start = time.monotonic()

        sources = sorted(landing_dir.glob(args.pattern))
        if args.max_files is not None:
            sources = sources[: args.max_files]

        scanned = len(sources)
        moved = 0
        skipped = 0

        for source_path in sources:
            ok = ingest_one(source_path=source_path, bronze_root=bronze_dir, dry_run=args.dry_run)
            if ok:
                moved += 1
            else:
                skipped += 1

        result = IngestResult(scanned=scanned, moved=moved, skipped=skipped)
        if args.interval_seconds is None:
            print(json.dumps(result.__dict__, separators=(",", ":")))
            return 0

        print(json.dumps({"iteration": iteration, **result.__dict__}, separators=(",", ":")))
        if args.iterations is not None and iteration >= args.iterations:
            return 0

        elapsed = time.monotonic() - iteration_start
        sleep_seconds = args.interval_seconds - elapsed
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())

