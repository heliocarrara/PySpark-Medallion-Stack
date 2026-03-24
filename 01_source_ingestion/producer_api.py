"""
Producer: XLM Price Ingestion from Public Exchange APIs.

This script fetches XLM ticker/price data from public endpoints (no auth)
and writes each API response as a JSON file into data_lake/landing so it can be
picked up by the bronze ingestion step.

Usage:
    python producer_api.py
    python producer_api.py --output-dir ..\data_lake\landing --timeout-seconds 15 --retries 2
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


EXCHANGE_ENDPOINTS: dict[str, str] = {
    "binance": "https://api.binance.com/api/v3/ticker/24hr?symbol=XLMUSDT",
    "kraken": "https://api.kraken.com/0/public/Ticker?pair=XLMUSD",
    "okx": "https://www.okx.com/api/v5/market/ticker?instId=XLM-USDT",
    "bybit": "https://api.bybit.com/v5/market/tickers?category=spot&symbol=XLMUSDT",
}


@dataclass(frozen=True)
class FetchSummary:
    scanned: int
    succeeded: int
    failed: int


def _to_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _extract_last_price(*, exchange: str, payload: object) -> tuple[float | None, str | None, str | None]:
    if exchange == "binance":
        if not isinstance(payload, dict):
            return None, None, "payload_not_dict"
        return _to_float(payload.get("lastPrice")), "USDT", None

    if exchange == "kraken":
        if not isinstance(payload, dict):
            return None, None, "payload_not_dict"
        errors = payload.get("error")
        if isinstance(errors, list) and errors:
            return None, None, f"kraken_error={errors[0]!r}"
        result = payload.get("result")
        if not isinstance(result, dict) or not result:
            return None, None, "kraken_missing_result"
        first_key = next(iter(result.keys()))
        ticker = result.get(first_key)
        if not isinstance(ticker, dict):
            return None, None, "kraken_ticker_not_dict"
        close = ticker.get("c")
        if not (isinstance(close, list) and len(close) >= 1):
            return None, None, "kraken_missing_close"
        return _to_float(close[0]), "USD", None

    if exchange == "okx":
        if not isinstance(payload, dict):
            return None, None, "payload_not_dict"
        data = payload.get("data")
        if not (isinstance(data, list) and data):
            return None, None, "okx_missing_data"
        first = data[0]
        if not isinstance(first, dict):
            return None, None, "okx_data0_not_dict"
        return _to_float(first.get("last")), "USDT", None

    if exchange == "bybit":
        if not isinstance(payload, dict):
            return None, None, "payload_not_dict"
        result = payload.get("result")
        if not isinstance(result, dict):
            return None, None, "bybit_missing_result"
        items = result.get("list")
        if not (isinstance(items, list) and items):
            return None, None, "bybit_missing_list"
        first = items[0]
        if not isinstance(first, dict):
            return None, None, "bybit_list0_not_dict"
        return _to_float(first.get("lastPrice")), "USDT", None

    return None, None, "unknown_exchange"


def _format_event_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _filename_timestamp(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _http_get_json(*, url: str, timeout_seconds: float) -> object:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "engineering-project-xlm/1.0",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        content_type = (resp.headers.get("Content-Type") or "").lower()
        raw = resp.read()
        text = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"Non-JSON response (content-type={content_type!r}): {e}") from e


def fetch_with_retries(
    *,
    exchange: str,
    url: str,
    timeout_seconds: float,
    retries: int,
    base_sleep_seconds: float,
) -> tuple[bool, object]:
    last_error: str | None = None
    for attempt in range(retries + 1):
        try:
            return True, _http_get_json(url=url, timeout_seconds=timeout_seconds)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError) as e:
            last_error = f"{type(e).__name__}: {e}"
            if attempt >= retries:
                break
            time.sleep(base_sleep_seconds * (2**attempt))
    return False, {"error": last_error, "exchange": exchange, "url": url}


def write_exchange_json(*, output_dir: Path, exchange: str, event_dt: datetime, payload: object) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"xlm_api_{exchange}_{_filename_timestamp(event_dt)}.json"
    output_path = output_dir / file_name

    last_price, quote_symbol, parse_error = _extract_last_price(exchange=exchange, payload=payload)
    envelope = {
        "asset_symbol": "XLM",
        "exchange": exchange,
        "url": EXCHANGE_ENDPOINTS.get(exchange),
        "event_ts": _format_event_ts(event_dt),
        "parsed": {
            "last_price": last_price,
            "quote_symbol": quote_symbol,
            "parse_error": parse_error,
        },
        "response": payload,
        "schema_version": "api_v1",
    }
    output_path.write_text(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch XLM tickers from public exchange APIs into data_lake/landing.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: <repo>/data_lake/landing).",
    )
    parser.add_argument(
        "--exchanges",
        type=str,
        default="binance,kraken,okx,bybit",
        help="Comma-separated list among: binance,kraken,okx,bybit",
    )
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="HTTP timeout per request.")
    parser.add_argument("--retries", type=int, default=2, help="Number of retries per exchange (in addition to first try).")
    parser.add_argument("--base-sleep-seconds", type=float, default=0.5, help="Base backoff sleep between retries.")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate endpoints by extracting last price (non-zero exit code if any fail).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help="If set, runs continuously, fetching every N seconds.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Optional number of iterations when running with --interval-seconds. If omitted, runs forever.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    output_dir: Path = args.output_dir if args.output_dir is not None else (repo_root / "data_lake" / "landing")

    requested = [e.strip().lower() for e in args.exchanges.split(",") if e.strip()]
    exchanges = [e for e in requested if e in EXCHANGE_ENDPOINTS]
    unknown = [e for e in requested if e not in EXCHANGE_ENDPOINTS]
    if unknown:
        raise SystemExit(f"Unknown exchanges: {', '.join(unknown)}")
    if not exchanges:
        raise SystemExit("--exchanges resolved to empty set")
    if args.retries < 0:
        raise SystemExit("--retries must be >= 0")
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be > 0")
    if args.base_sleep_seconds < 0:
        raise SystemExit("--base-sleep-seconds must be >= 0")
    if args.interval_seconds is not None and args.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be > 0")
    if args.iterations is not None and args.iterations < 1:
        raise SystemExit("--iterations must be >= 1")

    iteration = 0

    while True:
        iteration += 1
        iteration_start = time.monotonic()

        written: list[Path] = []
        succeeded = 0
        failed = 0
        validate_failed = 0

        for exchange in exchanges:
            url = EXCHANGE_ENDPOINTS[exchange]
            event_dt = datetime.now(timezone.utc)
            ok, payload = fetch_with_retries(
                exchange=exchange,
                url=url,
                timeout_seconds=args.timeout_seconds,
                retries=args.retries,
                base_sleep_seconds=args.base_sleep_seconds,
            )
            last_price, quote_symbol, parse_error = _extract_last_price(exchange=exchange, payload=payload)
            written.append(write_exchange_json(output_dir=output_dir, exchange=exchange, event_dt=event_dt, payload=payload))
            if ok:
                succeeded += 1
            else:
                failed += 1
            if args.validate:
                validation_row = {
                    "iteration": iteration,
                    "exchange": exchange,
                    "url": url,
                    "fetch_ok": ok,
                    "last_price": last_price,
                    "quote_symbol": quote_symbol,
                    "parse_error": parse_error,
                }
                print(json.dumps(validation_row, ensure_ascii=False, separators=(",", ":")))
                if (not ok) or last_price is None or parse_error is not None:
                    validate_failed += 1

        for path in written:
            print(path.as_posix())
        print(
            json.dumps(
                {
                    "iteration": iteration,
                    **FetchSummary(scanned=len(exchanges), succeeded=succeeded, failed=failed).__dict__,
                    "validate_failed": validate_failed if args.validate else None,
                },
                separators=(",", ":"),
            )
        )

        if args.interval_seconds is None:
            return 2 if (args.validate and validate_failed > 0) else 0

        if args.iterations is not None and iteration >= args.iterations:
            return 2 if (args.validate and validate_failed > 0) else 0

        elapsed = time.monotonic() - iteration_start
        sleep_seconds = args.interval_seconds - elapsed
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
