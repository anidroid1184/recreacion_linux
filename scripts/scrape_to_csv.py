from __future__ import annotations
import asyncio
import argparse
import csv
import logging
import os
import sys
from contextlib import suppress
from typing import Any

# Ensure project root is on sys.path when running from recreacion_linux/
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from recreacion_linux.logging_setup import setup_file_logging
from recreacion_linux.config import settings
from recreacion_linux.services.sheets_client import SheetsClient
from recreacion_linux.services.tracker_service import TrackerService
from recreacion_linux.web.inter_scraper_async import AsyncInterScraper
from oauth2client.service_account import ServiceAccountCredentials


def load_credentials() -> ServiceAccountCredentials:
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("CREDENTIALS_PATH")
    if env_path:
        env_path = os.path.normpath(os.path.expandvars(os.path.expanduser(env_path)))
        if os.path.isfile(env_path):
            logging.info("Using credentials from env path: %s", env_path)
            return ServiceAccountCredentials.from_json_keyfile_name(env_path, scope)
    # fallbacks
    pkg_dir = os.path.dirname(__file__)
    candidates = [
        os.path.join(PROJECT_ROOT, "credentials.json"),
        os.path.join(pkg_dir, "../credentials.json"),
        os.path.join(os.getcwd(), "credentials.json"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            logging.info("Using credentials from: %s", p)
            return ServiceAccountCredentials.from_json_keyfile_name(p, scope)
    raise FileNotFoundError("credentials.json not found; set GOOGLE_APPLICATION_CREDENTIALS or place it at project root")


async def scrape_to_csv(
    out_csv: str,
    count: int = 4,
    start_row: int = 2,
    headless: bool | None = None,
    max_concurrency: int = 1,
    rps: float | None = 0.6,
    retries: int = 2,
    timeout_ms: int = 60000,
) -> str:
    """Read first N tracking numbers from the sheet and write results to a CSV.

    CSV columns: row, tracking, status_raw, status_normalized
    """
    creds = load_credentials()
    sheets = SheetsClient(creds, settings.spreadsheet_name)

    # Read sheet data
    records = sheets.read_main_records_resilient()
    headers = sheets.read_headers()

    try:
        tn_col_idx = headers.index("ID TRACKING")
    except ValueError:
        raise RuntimeError("Header 'ID TRACKING' not found in sheet")

    # Build items from first rows
    items: list[tuple[int, str]] = []  # (row_index, tracking)
    for idx, rec in enumerate(records, start=2):
        if idx < start_row:
            continue
        tn = str(rec.get("ID TRACKING", "")).strip()
        if tn:
            items.append((idx, tn))
        if len(items) >= count:
            break

    if not items:
        logging.warning("No tracking numbers found to process")
        # still create an empty CSV
        os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["row", "tracking", "status_raw", "status_normalized"])
        return out_csv

    # Resolve headless flag: env or settings fallback
    if headless is None:
        try:
            headless_flag = bool(settings.HEADLESS)  # type: ignore[attr-defined]
        except Exception:
            headless_env = os.getenv("HEADLESS", "true").strip().lower()
            headless_flag = headless_env in {"1", "true", "yes"}
    else:
        headless_flag = headless

    # Debug flags from env
    debug_flag = os.getenv("DEBUG_SCRAPER", "false").strip().lower() in {"1", "true", "yes"}
    block_flag = os.getenv("BLOCK_RESOURCES", "true").strip().lower() not in {"0", "false", "no"}
    logging.info("Scraper flags: DEBUG_SCRAPER=%s, BLOCK_RESOURCES=%s, HEADLESS=%s", debug_flag, block_flag, headless_flag)

    scraper = AsyncInterScraper(
        headless=headless_flag,
        max_concurrency=max_concurrency,
        slow_mo=0,
        retries=retries,
        timeout_ms=timeout_ms,
        block_resources=block_flag,
        debug=debug_flag,
    )
    await scraper.start()

    try:
        tn_list = [tn for _, tn in items]
        results = await scraper.get_status_many(tn_list, rps=rps)
        status_by_tn = {tn: (raw or "").strip() for tn, raw in results}

        # Second pass for blanks only
        missing = [tn for tn in tn_list if not status_by_tn.get(tn)]
        if missing:
            logging.info("Second pass for %d missing", len(missing))
            results2 = await scraper.get_status_many(missing, rps=(rps or 0.6))
            for tn, raw in results2:
                if raw:
                    status_by_tn[tn] = raw.strip()

        # Write CSV
        os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["row", "tracking", "status_raw", "status_normalized"])
            for row_idx, tn in items:
                raw = status_by_tn.get(tn, "")
                norm = TrackerService.normalize_status(raw) if raw else ""
                writer.writerow([row_idx, tn, raw, norm])
        logging.info("Wrote CSV: %s", out_csv)
        return out_csv
    finally:
        await scraper.close()


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Standalone scraping to CSV for a small sample")
    p.add_argument("--out", dest="out_csv", type=str, default="out/sample_statuses.csv")
    p.add_argument("--count", type=int, default=4)
    p.add_argument("--start-row", type=int, default=2)
    p.add_argument("--headless", type=str, default=None, help="true/false to override env/settings")
    p.add_argument("--max-concurrency", type=int, default=1)
    p.add_argument("--rps", type=float, default=0.6)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--timeout-ms", dest="timeout_ms", type=int, default=60000)
    return p


def str2bool(v: str | None) -> bool | None:
    if v is None:
        return None
    v2 = str(v).strip().lower()
    if v2 in {"1", "true", "yes", "y"}:
        return True
    if v2 in {"0", "false", "no", "n"}:
        return False
    return None


def main() -> int:
    # Log to file to keep terminal clean; you can also tail the log
    log_path = setup_file_logging()
    logging.info("Scrape-to-CSV runner started. Log file: %s", log_path)

    try:
        parser = _build_parser()
        args = parser.parse_args()
        headless_override = str2bool(args.headless)
        out = asyncio.run(scrape_to_csv(
            out_csv=args.out_csv,
            count=args.count,
            start_row=args.start_row,
            headless=headless_override,
            max_concurrency=args.max_concurrency,
            rps=args.rps,
            retries=args.retries,
            timeout_ms=args.timeout_ms,
        ))
        print(out)
        return 0
    except Exception as e:
        logging.exception("Fatal error in scrape_to_csv: %s", e)
        return 2
    finally:
        logging.info("Scrape-to-CSV runner finished")


if __name__ == "__main__":
    raise SystemExit(main())
