#!/usr/bin/env python3
"""
Download historical data for a list of instrument keys from an Upstox-style REST API.

This script is intentionally generic: supply the API base URL and endpoint path, and
an access token via the UPSTOX_TOKEN environment variable.

It reads instrument keys from a file (one per line) or from command-line args, and
saves each instrument's raw JSON response to the output directory. Optionally tries
to write a simple CSV if the response contains a top-level array of candle-like
objects or a `candles` key.

Usage examples are in the repository README.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests


def parse_args():
    p = argparse.ArgumentParser(description="Download historical data for instrument keys")
    p.add_argument("--keys-file", help="Path to file with one instrument_key per line")
    p.add_argument("--key", action="append", help="Instrument key (can be repeated)")
    p.add_argument("--start", required=True, help="Start timestamp or date (ISO8601 or epoch ms)")
    p.add_argument("--end", required=True, help="End timestamp or date (ISO8601 or epoch ms)")
    # For Upstox V3 we request: /historical-candle/:instrument_key/:unit/:interval/:to_date/:from_date
    p.add_argument("--unit", default="minutes", help="Unit for V3 API: minutes|hours|days|weeks|months")
    p.add_argument("--interval", default="1", help="Interval value for V3 API (numeric). For minutes use 1..300")
    p.add_argument("--base-url", default=os.environ.get("UPSTOX_BASE_URL", "https://api.upstox.com/v3"),
                   help="Base API URL")
    p.add_argument("--config", default="config.json", help="Path to JSON config file containing token/base_url")
    p.add_argument("--endpoint", default="historical-candle", help="Endpoint path (appended to base URL). Keep as 'historical-candle' for V3")
    p.add_argument("--out-dir", default="data/quotes", help="Output directory for saved files")
    p.add_argument("--token-env", default="UPSTOX_TOKEN", help="Environment variable name for token")
    p.add_argument("--sleep", type=float, default=0.2, help="Seconds to sleep between requests to avoid rate limits")
    p.add_argument("--retries", type=int, default=3, help="Number of retries for transient failures")
    p.add_argument("--timeout", type=float, default=10.0, help="HTTP request timeout in seconds")
    return p.parse_args()


def to_millis(ts: str) -> str:
    # Try to detect ISO date and convert to epoch ms, otherwise pass through
    try:
        if ts.isdigit():
            return ts
        dt = datetime.fromisoformat(ts)
        return str(int(dt.timestamp() * 1000))
    except Exception:
        return ts


def to_yyyy_mm_dd(ts: str) -> str:
    # Accept epoch ms or ISO date and return YYYY-MM-DD
    try:
        if ts.isdigit():
            ms = int(ts)
            dt = datetime.fromtimestamp(ms / 1000.0)
            return dt.strftime('%Y-%m-%d')
        dt = datetime.fromisoformat(ts)
        return dt.strftime('%Y-%m-%d')
    except Exception:
        # last resort: if already YYYY-MM-DD like, return as-is
        return ts


def ensure_out_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    # Simple CSV writer assuming rows is a list of dicts with same keys
    if not rows:
        return
    keys = list(rows[0].keys())
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(keys) + "\n")
        for r in rows:
            f.write("\t".join(str(r.get(k, "")) for k in keys) + "\n")


def fetch_with_retries(session: requests.Session, url: str, headers: Dict[str, str], params: Dict[str, str],
                       retries: int, timeout: float) -> Optional[requests.Response]:
    backoff = 1.0
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp
            # For rate limit or server errors, retry
            if resp.status_code in (429, 500, 502, 503, 504):
                print(f"Transient HTTP {resp.status_code} for {params.get('instrument_key')} - attempt {attempt}")
            else:
                print(f"HTTP {resp.status_code} for {params.get('instrument_key')}: {resp.text}")
                return resp
        except requests.RequestException as e:
            print(f"Request error on attempt {attempt} for {params.get('instrument_key')}: {e}")
        time.sleep(backoff)
        backoff *= 2
    return None


def main():
    args = parse_args()
    token = os.environ.get(args.token_env)
    # If token not in env, try config file (JSON)
    if not token and args.config:
        try:
            with open(args.config, "r", encoding="utf-8") as cf:
                cfg = json.load(cf)
                # support a few keys
                token = cfg.get("token") or cfg.get("upstox_token") or (cfg.get("upstox") and cfg.get("upstox").get("token"))
                # allow base_url override from config
                if not args.base_url and cfg.get("base_url"): args.base_url = cfg.get("base_url")
        except FileNotFoundError:
            # config missing is fine; we'll error below
            pass
        except Exception as e:
            print(f"Failed to read config {args.config}: {e}")

    if not token:
        print(f"Missing access token: set environment variable {args.token_env} or provide it in {args.config}")
        sys.exit(2)

    keys = []
    if args.keys_file:
        with open(args.keys_file, "r", encoding="utf-8") as f:
            keys.extend(line.strip() for line in f if line.strip())
    if args.key:
        keys.extend(args.key)
    if not keys:
        print("No instrument keys provided (use --keys-file or --key)")
        sys.exit(2)

    ensure_out_dir(args.out_dir)
    session = requests.Session()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Keep both representations: epoch-ms (used for filenames) and YYYY-MM-DD (used by V3 path)
    start_ms = to_millis(args.start)
    end_ms = to_millis(args.end)
    start_date = to_yyyy_mm_dd(args.start)
    end_date = to_yyyy_mm_dd(args.end)

    import urllib.parse
    # Decide behavior for V3 historical-candle path: path params rather than query params
    for ik in keys:
        safe_key = ik.replace('|', '_')
        encoded_key = urllib.parse.quote(ik, safe='')

        # Build requests in chunks for large minute-range requests to avoid API limits
        unit = args.unit
        interval = str(args.interval)

        # Determine chunk size (days) based on unit/interval. For minutes with small intervals, restrict to ~30 days.
        if unit.startswith('minutes') or unit.startswith('minute'):
            try:
                ival = int(interval)
            except Exception:
                ival = 1
            if ival <= 15:
                chunk_days = 30
            else:
                # larger minute intervals allow longer ranges; be conservative
                chunk_days = 90
        elif unit.startswith('hours'):
            chunk_days = 90
        else:
            # days/weeks/months — allow full range
            chunk_days = None

        # iterate from start_date to end_date in chunks
        cur_start = datetime.fromisoformat(start_date)
        final_end = datetime.fromisoformat(end_date)

        while cur_start <= final_end:
            if chunk_days is None:
                chunk_end_dt = final_end
            else:
                chunk_end_dt = cur_start + timedelta(days=chunk_days - 1)
                if chunk_end_dt > final_end:
                    chunk_end_dt = final_end

            from_str = cur_start.strftime('%Y-%m-%d')
            to_str = chunk_end_dt.strftime('%Y-%m-%d')

            # V3 path expects: /historical-candle/:instrument_key/:unit/:interval/:to_date/:from_date
            url = f"{args.base_url.rstrip('/')}/{args.endpoint.lstrip('/')}/{encoded_key}/{unit}/{interval}/{to_str}/{from_str}"
            print(f"Fetching {ik} -> {url}")

            # For fetch_with_retries keep params empty and pass URL directly
            resp = fetch_with_retries(session, url, headers, {}, retries=args.retries, timeout=args.timeout)
            if resp is None:
                print(f"Failed to fetch data for {ik} chunk {from_str}->{to_str} after {args.retries} attempts")
                cur_start = chunk_end_dt + timedelta(days=1)
                continue

            # prepare output filenames using chunk dates
            out_json = os.path.join(args.out_dir, f"{safe_key}_{from_str}_{to_str}.json")
            try:
                data = resp.json()
            except Exception:
                # Fallback: save raw text
                with open(out_json, "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"Saved raw response for {ik} to {out_json}")
                time.sleep(args.sleep)
                cur_start = chunk_end_dt + timedelta(days=1)
                continue

            save_json(out_json, data)
            print(f"Saved JSON for {ik} -> {out_json}")

            # Save metadata to allow deterministic mapping back to the original instrument_key
            meta = {
                "instrument_key": ik,
                "fetched_at": int(time.time() * 1000),
                "start": from_str,
                "end": to_str,
                "interval": f"{unit}/{interval}",
                "source_url": url,
            }
            meta_path = os.path.join(args.out_dir, f"{safe_key}_{from_str}_{to_str}.meta.json")
            save_json(meta_path, meta)
            print(f"Saved metadata for {ik} -> {meta_path}")

            # If data looks like candles, save TSV for easy loading (v3 returns data.candles as arrays)
            rows = None
            if isinstance(data, dict) and data.get('data') and isinstance(data['data'].get('candles'), list):
                # convert candles arrays to dict rows with known columns
                rows = []
                for c in data['data']['candles']:
                    # [timestamp, open, high, low, close, volume, oi]
                    row = {
                        'timestamp': c[0], 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4], 'volume': c[5]
                    }
                    # include oi if present
                    if len(c) > 6:
                        row['oi'] = c[6]
                    rows.append(row)

            if rows:
                out_csv = os.path.join(args.out_dir, f"{safe_key}_{from_str}_{to_str}.tsv")
                save_csv(out_csv, rows)
                print(f"Saved TSV for {ik} -> {out_csv}")

            time.sleep(args.sleep)

            # advance to next chunk
            cur_start = chunk_end_dt + timedelta(days=1)


if __name__ == "__main__":
    main()
