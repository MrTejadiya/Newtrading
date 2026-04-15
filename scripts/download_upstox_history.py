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
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests


def parse_args():
    p = argparse.ArgumentParser(description="Download historical data for instrument keys")
    p.add_argument("--keys-file", help="Path to file with one instrument_key per line")
    p.add_argument("--key", action="append", help="Instrument key (can be repeated)")
    p.add_argument("--start", required=True, help="Start timestamp or date (ISO8601 or epoch ms)")
    p.add_argument("--end", required=True, help="End timestamp or date (ISO8601 or epoch ms)")
    p.add_argument("--interval", default="1minute", help="Interval/granularity (dependent on API)")
    p.add_argument("--base-url", default=os.environ.get("UPSTOX_BASE_URL", "https://api.upstox.com/v3"),
                   help="Base API URL")
    p.add_argument("--endpoint", default="historical", help="Endpoint path (appended to base URL)")
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
    if not token:
        print(f"Missing access token: set environment variable {args.token_env}")
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

    start = to_millis(args.start)
    end = to_millis(args.end)

    for ik in keys:
        params = {
            "instrument_key": ik,
            "start": start,
            "end": end,
            "interval": args.interval,
        }

        url = args.base_url.rstrip("/") + "/" + args.endpoint.lstrip("/")
        print(f"Fetching {ik} -> {url} params={params}")

        resp = fetch_with_retries(session, url, headers, params, retries=args.retries, timeout=args.timeout)
        if resp is None:
            print(f"Failed to fetch data for {ik} after {args.retries} attempts")
            continue

        safe_key = ik.replace('|', '_')
        out_json = os.path.join(args.out_dir, f"{safe_key}_{start}_{end}.json")
        try:
            data = resp.json()
        except Exception:
            # Fallback: save raw text
            with open(out_json, "w", encoding="utf-8") as f:
                f.write(resp.text)
            print(f"Saved raw response for {ik} to {out_json}")
            time.sleep(args.sleep)
            continue

        save_json(out_json, data)
        print(f"Saved JSON for {ik} -> {out_json}")

        # Save metadata to allow deterministic mapping back to the original instrument_key
        meta = {
            "instrument_key": ik,
            "fetched_at": int(time.time() * 1000),
            "start": start,
            "end": end,
            "interval": args.interval,
            "source_url": url,
        }
        meta_path = os.path.join(args.out_dir, f"{safe_key}_{start}_{end}.meta.json")
        save_json(meta_path, meta)
        print(f"Saved metadata for {ik} -> {meta_path}")

        # If data looks like candles, try to save TSV for easy loading
        rows = None
        if isinstance(data, list):
            # assume list of dicts
            rows = data
        elif isinstance(data, dict) and "candles" in data and isinstance(data["candles"], list):
            rows = data["candles"]

        if rows:
            out_csv = os.path.join(args.out_dir, f"{ik.replace('|','_')}_{start}_{end}.tsv")
            save_csv(out_csv, rows)
            print(f"Saved TSV for {ik} -> {out_csv}")

        time.sleep(args.sleep)


if __name__ == "__main__":
    main()
