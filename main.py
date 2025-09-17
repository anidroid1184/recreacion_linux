from __future__ import annotations
import argparse
import asyncio
import csv
import logging
import os
import sys
from contextlib import suppress
from typing import Any, Iterable

# Ensure project root is on sys.path when running from recreacion_linux/
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from oauth2client.service_account import ServiceAccountCredentials
from recreacion_linux.config import settings
from recreacion_linux.services.sheets_client import SheetsClient
from recreacion_linux.web.inter_scraper_async import AsyncInterScraper
from recreacion_linux.logging_setup import setup_file_logging
from recreacion_linux.services.tracker_service import TrackerService
from recreacion_linux.comparer import compare_statuses
from recreacion_linux.report import generate_daily_report


def load_credentials() -> ServiceAccountCredentials:
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]

    # Prefer explicit env vars
    env_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("CREDENTIALS_PATH")
    if env_path:
        env_path = os.path.normpath(os.path.expandvars(os.path.expanduser(env_path)))
        if os.path.isfile(env_path):
            logging.info("Using credentials from env path: %s", env_path)
            return ServiceAccountCredentials.from_json_keyfile_name(env_path, scope)

    # Candidate fallback paths
    pkg_dir = os.path.dirname(__file__)
    cwd = os.getcwd()
    candidates = [
        os.path.join(pkg_dir, "credentials.json"),          # running from recreacion_linux/
        os.path.join(PROJECT_ROOT, "credentials.json"),     # parent project root
        os.path.join(cwd, "credentials.json"),              # current working directory
    ]
    for path in candidates:
        if os.path.isfile(path):
            logging.info("Using credentials from: %s", path)
            return ServiceAccountCredentials.from_json_keyfile_name(path, scope)

    logging.error(
        "credentials.json not found. Tried env GOOGLE_APPLICATION_CREDENTIALS/CREDENTIALS_PATH and paths: %s",
        candidates,
    )
    raise FileNotFoundError("credentials.json not found; place it at project root or set GOOGLE_APPLICATION_CREDENTIALS")


def _flush_batch(sheets: SheetsClient, batch_updates: list[tuple[int, list[Any]]]):
    """Write only the exact cells that changed (single-column updates batched).

    Groups updates by column index and splits into consecutive row blocks.
    This preserves other columns and reduces API calls.
    """
    if not batch_updates:
        return

    # Build mapping: col_idx -> list[(row, value)]
    by_col: dict[int, list[tuple[int, Any]]] = {}
    for row, arr in batch_updates:
        for col_idx, val in enumerate(arr, start=1):
            if val is None:
                continue
            by_col.setdefault(col_idx, []).append((row, val))

    batched_payload: list[dict] = []
    for col_idx, items in by_col.items():
        items.sort(key=lambda x: x[0])
        block: list[tuple[int, Any]] = []
        prev_row = None

        def flush_block():
            if not block:
                return
            start_row = block[0][0]
            end_row = block[-1][0]
            values = [[v] for _, v in block]  # single column
            col_letter = chr(ord('A') + col_idx - 1)
            a1 = f"{col_letter}{start_row}:{col_letter}{end_row}"
            batched_payload.append({"range": a1, "values": values})

        for r, v in items:
            if prev_row is None or r == prev_row + 1:
                block.append((r, v))
            else:
                flush_block()
                block = [(r, v)]
            prev_row = r
        flush_block()

    if batched_payload:
        CHUNK = 100
        for i in range(0, len(batched_payload), CHUNK):
            chunk = batched_payload[i:i+CHUNK]
            sheets.values_batch_update(chunk)


def mark_compare_column(
    sheets: SheetsClient,
    start_row: int = 2,
    end_row: int | None = None,
) -> int:
    """Write per-row comparison into existing columns E and F.

    - Column E: header 'COINCIDEN' (boolean as string 'TRUE'/'FALSE')
      TRUE if normalized DROPi == normalized TRACKING and both non-empty; FALSE otherwise.
    - Column F: header 'ALERTA' computed via TrackerService.compute_alert(dropi_norm, web_norm)

    Does not add or rename headers; it writes to the columns already present.
    Returns number of rows written.
    """
    records = sheets.read_main_records_resilient()
    headers = sheets.read_headers()

    # Resolve required base columns by name
    try:
        web_col_name = "STATUS TRACKING"
        dropi_col_name = "STATUS DROPI"
        dropi_col = headers.index(dropi_col_name) + 1
        web_col = headers.index(web_col_name) + 1
    except ValueError:
        logging.error("Mark-compare: required headers not found (STATUS DROPI / STATUS TRACKING)")
        return 0

    # Resolve destination columns: prefer exact headers, fallback to fixed E(5), F(6)
    def _find_col(candidates: list[str], fallback_index: int) -> int:
        for name in candidates:
            if name in headers:
                return headers.index(name) + 1
        return fallback_index

    coincide_col = _find_col(["COINCIDEN", "COINCIDE", "Coinciden", "Coincide"], 5)  # E
    alerta_col = _find_col(["ALERTA", "Alerta"], 6)  # F

    updates: list[tuple[int, list[Any]]] = []

    for idx, rec in enumerate(records, start=2):
        if idx < start_row:
            continue
        if end_row is not None and idx > end_row:
            break

        dropi_raw = str(rec.get("STATUS DROPI", "")).strip()
        web_raw = str(rec.get("STATUS TRACKING", "")).strip()

        dropi_norm = TrackerService.normalize_status(dropi_raw) if dropi_raw else ""
        web_norm = TrackerService.normalize_status(web_raw) if web_raw else ""

        coincide_val = "TRUE" if (dropi_norm and web_norm and dropi_norm == web_norm) else "FALSE"
        alerta_val = TrackerService.compute_alert(dropi_norm or "", web_norm or "")

        # Build row updates up to the furthest of E/F
        max_col = max(coincide_col, alerta_col)
        row_updates = [None] * max_col
        row_updates[coincide_col - 1] = coincide_val
        row_updates[alerta_col - 1] = alerta_val
        updates.append((idx, row_updates))

    if not updates:
        logging.info("Mark-compare: no rows to update")
        return 0

    _flush_batch(sheets, updates)
    logging.info("Mark-compare written rows: %d (cols E/F)", len(updates))
    return len(updates)


async def update_statuses_linux(
    sheets: SheetsClient,
    headless: bool = True,
    start_row: int = 2,
    end_row: int | None = None,
    only_empty: bool = False,
    max_concurrency: int = 2,
    rps: float | None = 0.8,
    retries: int = 1,
    timeout_ms: int = 25000,
    batch_size: int = 1500,
    sleep_between_batches: float = 15.0,
) -> None:
    """Linux-optimized updater using AsyncInterScraper with low memory footprint.

    - Uses small concurrency and throttling to fit in ~4GB RAM environments.
    - Blocks heavy resources (images, media, fonts, CSS) to speed up page load.
    - Writes only non-empty statuses to avoid overwriting existing data.
    """
    # Read sheet
    records = sheets.read_main_records_resilient()
    headers = sheets.read_headers()

    # Ensure required headers exist
    required_headers = ["ID DROPI", "ID TRACKING", "STATUS DROPI", "STATUS TRACKING", "Alerta"]
    sheets.ensure_headers(required_headers)
    headers = sheets.read_headers()

    web_col = headers.index("STATUS TRACKING") + 1
    raw_col = headers.index("STATUS TRACKING RAW") + 1 if "STATUS TRACKING RAW" in headers else None
    dropi_col = headers.index("STATUS DROPI") + 1 if "STATUS DROPI" in headers else None
    alerta_col = headers.index("Alerta") + 1 if "Alerta" in headers else None

    # Prepare items
    items: list[tuple[int, str]] = []  # (row, tracking)
    for idx, rec in enumerate(records, start=2):
        if idx < start_row:
            continue
        if end_row is not None and idx > end_row:
            break
        tn = str(rec.get("ID TRACKING", "")).strip()
        if not tn:
            continue
        if only_empty:
            current_web = str(rec.get("STATUS TRACKING", "")).strip()
            if current_web:
                continue
        items.append((idx, tn))

    if not items:
        logging.info("No rows to process based on current filters")
        return

    # Helper to chunk
    def chunk(seq: list[tuple[int, str]], size: int):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]

    processed_total = 0

    # Single browser instance across all batches to reduce overhead
    # Debug and resource flags from environment (default: debug off, block resources on)
    debug_flag = os.getenv("DEBUG_SCRAPER", "false").strip().lower() in {"1", "true", "yes"}
    block_flag = os.getenv("BLOCK_RESOURCES", "true").strip().lower() not in {"0", "false", "no"}
    logging.info("Scraper flags: DEBUG_SCRAPER=%s, BLOCK_RESOURCES=%s", debug_flag, block_flag)

    # Robust headless resolution (support older configs that may miss HEADLESS)
    try:
        headless_flag = bool(settings.HEADLESS)  # type: ignore[attr-defined]
    except Exception:
        headless_env = os.getenv("HEADLESS", "true").strip().lower()
        headless_flag = headless_env in {"1", "true", "yes"}
    logging.info("Effective HEADLESS=%s", headless_flag)

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
        for batch_idx, batch in enumerate(chunk(items, max(1, int(batch_size))), start=1):
            first_row = batch[0][0]
            last_row = batch[-1][0]
            logging.info("[Batch %d] rows %s-%s, items=%d", batch_idx, first_row, last_row, len(batch))

            tn_list = [tn for _, tn in batch]
            results = await scraper.get_status_many(tn_list, rps=rps)
            status_by_tn = {tn: raw for tn, raw in results}

            # Quick second pass for blanks in this batch (keep same constraints)
            missing = [tn for tn in tn_list if not (status_by_tn.get(tn) or "").strip()]
            if missing:
                logging.info("[Batch %d] second pass for %d missing", batch_idx, len(missing))
                results2 = await scraper.get_status_many(missing, rps=(rps or 0.6))
                for tn, raw in results2:
                    if raw:
                        status_by_tn[tn] = raw

            # Build batched updates
            batch_updates: list[tuple[int, list[Any]]] = []
            for (row_idx, tn) in batch:
                raw = (status_by_tn.get(tn) or "").strip()
                if not raw:
                    continue

                # Normalize using TrackerService mapping
                norm = TrackerService.normalize_status(raw)

                # Build row updates up to the furthest column we'll touch
                max_col = web_col
                if raw_col:
                    max_col = max(max_col, raw_col)
                if alerta_col:
                    max_col = max(max_col, alerta_col)
                row_updates = [None] * max_col

                # Write normalized tracking status
                row_updates[web_col - 1] = norm

                # Optionally write raw into STATUS TRACKING RAW if present
                if raw_col:
                    row_updates[raw_col - 1] = raw

                # Optionally recompute Alerta if both DROPi and normalized tracking are available
                if alerta_col:
                    # Read DROPi from current records array (we have it as rec) using index from headers
                    try:
                        # records is aligned with row indices starting at 2
                        rec = records[row_idx - 2]
                        dropi_raw = str(rec.get("STATUS DROPI", "")).strip()
                        dropi_norm = TrackerService.normalize_status(dropi_raw) if dropi_raw else ""
                        alerta = TrackerService.compute_alert(dropi_norm or "", norm or "")
                        row_updates[alerta_col - 1] = alerta
                    except Exception:
                        # Be permissive: if anything fails, skip alerta update for this row
                        pass

                batch_updates.append((row_idx, row_updates))

            if batch_updates:
                _flush_batch(sheets, batch_updates)

            processed_total += len(batch)
            logging.info("[Batch %d] done. processed_total=%d", batch_idx, processed_total)

            if sleep_between_batches and processed_total < len(items):
                with suppress(Exception):
                    await asyncio.sleep(float(sleep_between_batches))
    finally:
        await scraper.close()


def str2bool(v: str) -> bool:
    return str(v).lower() in {"1", "true", "yes", "y"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Linux-optimized processes (headless, file-logs only)"
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # scrape
    p_scrape = sub.add_parser("scrape", help="Scrape Inter and write statuses to sheet")
    p_scrape.add_argument("--start-row", type=int, default=2)
    p_scrape.add_argument("--end-row", type=int, default=None)
    p_scrape.add_argument("--only-empty", type=str2bool, default=True)
    p_scrape.add_argument("--max-concurrency", type=int, default=2)
    p_scrape.add_argument("--rps", type=float, default=0.8)
    p_scrape.add_argument("--retries", type=int, default=1)
    p_scrape.add_argument("--timeout-ms", dest="timeout_ms", type=int, default=25000)
    p_scrape.add_argument("--batch-size", type=int, default=1500)
    p_scrape.add_argument("--sleep-between-batches", type=float, default=15.0)

    # scrape-to-csv (standalone sample to CSV)
    p_scrape_csv = sub.add_parser("scrape-to-csv", help="Scrape a small sample and write results to a local CSV (no sheet writes)")
    p_scrape_csv.add_argument("--out", dest="out_csv", type=str, default="recreacion_linux/out/sample_statuses.csv")
    p_scrape_csv.add_argument("--count", type=int, default=4)
    p_scrape_csv.add_argument("--start-row", type=int, default=2)
    p_scrape_csv.add_argument("--max-concurrency", type=int, default=1)
    p_scrape_csv.add_argument("--rps", type=float, default=0.6)
    p_scrape_csv.add_argument("--retries", type=int, default=2)
    # Increase timeout for slower iframe mounting (anti-bot, network)
    p_scrape_csv.add_argument("--timeout-ms", dest="timeout_ms", type=int, default=120000)
    # Soft anti-bot pacing even in headless mode
    p_scrape_csv.add_argument("--slow-mo", dest="slow_mo", type=int, default=100)

    # compare
    p_compare = sub.add_parser("compare", help="Compare DROPi vs WEB statuses and print count; results used by report")
    p_compare.add_argument("--start-row", type=int, default=2)
    p_compare.add_argument("--end-row", type=int, default=None)
    p_compare.add_argument("--only-mismatches", type=str2bool, default=True)

    # report
    p_report = sub.add_parser("report", help="Generate/append daily report sheet with mismatches")
    p_report.add_argument("--start-row", type=int, default=2)
    p_report.add_argument("--end-row", type=int, default=None)
    p_report.add_argument("--only-mismatches", type=str2bool, default=True)
    p_report.add_argument("--prefix", type=str, default=None)

    # mark-compare (write TRUE/FALSE in 'COINCIDE' for all rows)
    p_mark = sub.add_parser("mark-compare", help="Write per-row boolean 'COINCIDE' across all rows")
    p_mark.add_argument("--start-row", type=int, default=2)
    p_mark.add_argument("--end-row", type=int, default=None)

    # all
    p_all = sub.add_parser("all", help="Run scrape then report")
    p_all.add_argument("--start-row", type=int, default=2)
    p_all.add_argument("--end-row", type=int, default=None)
    p_all.add_argument("--only-empty", type=str2bool, default=True)
    p_all.add_argument("--max-concurrency", type=int, default=2)
    p_all.add_argument("--rps", type=float, default=0.8)
    p_all.add_argument("--retries", type=int, default=1)
    p_all.add_argument("--timeout-ms", dest="timeout_ms", type=int, default=25000)
    p_all.add_argument("--batch-size", type=int, default=1500)
    p_all.add_argument("--sleep-between-batches", type=float, default=15.0)
    p_all.add_argument("--only-mismatches", type=str2bool, default=True)
    p_all.add_argument("--prefix", type=str, default=None)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    log_path = setup_file_logging()
    logging.info("Linux runner started. Log file: %s", log_path)

    try:
        creds = load_credentials()
        sheets = SheetsClient(creds, settings.spreadsheet_name)

        if args.command == "scrape":
            asyncio.run(update_statuses_linux(
                sheets,
                headless=True,
                start_row=args.start_row,
                end_row=args.end_row,
                only_empty=args.only_empty,
                max_concurrency=args.max_concurrency,
                rps=args.rps,
                retries=args.retries,
                timeout_ms=args.timeout_ms,
                batch_size=args.batch_size,
                sleep_between_batches=args.sleep_between_batches,
            ))
            logging.info("Scrape finished successfully")
            return 0

        if args.command == "scrape-to-csv":
            # Detailed, step-by-step logs
            logging.info("[scrape-to-csv] Starting. out=%s count=%s start_row=%s", args.out_csv, args.count, args.start_row)
            # Read sheet data
            records = sheets.read_main_records_resilient()
            headers = sheets.read_headers()
            logging.info("[scrape-to-csv] Headers detected: %s", headers)

            # Resolve tracking column
            try:
                tn_col_idx = headers.index("ID TRACKING")
                logging.info("[scrape-to-csv] Found 'ID TRACKING' at index %d", tn_col_idx)
            except ValueError:
                logging.error("[scrape-to-csv] Header 'ID TRACKING' not found")
                return 2

            # Build items from first rows
            items: list[tuple[int, str]] = []
            for idx, rec in enumerate(records, start=2):
                if idx < args.start_row:
                    continue
                tn = str(rec.get("ID TRACKING", "")).strip()
                if tn:
                    items.append((idx, tn))
                if len(items) >= int(args.count):
                    break
            logging.info("[scrape-to-csv] Sample size: %d", len(items))
            if not items:
                logging.warning("[scrape-to-csv] No tracking numbers found to process")
                os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
                with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["row", "tracking", "status_raw", "status_normalized"])
                print(args.out_csv)
                return 0

            # Resolve flags
            debug_flag = os.getenv("DEBUG_SCRAPER", "false").strip().lower() in {"1", "true", "yes"}
            block_flag = os.getenv("BLOCK_RESOURCES", "true").strip().lower() not in {"0", "false", "no"}
            try:
                headless_flag = bool(settings.HEADLESS)  # type: ignore[attr-defined]
            except Exception:
                headless_env = os.getenv("HEADLESS", "true").strip().lower()
                headless_flag = headless_env in {"1", "true", "yes"}
            logging.info("[scrape-to-csv] Flags: DEBUG_SCRAPER=%s BLOCK_RESOURCES=%s HEADLESS=%s", debug_flag, block_flag, headless_flag)

            async def _run() -> None:
                scraper = AsyncInterScraper(
                    headless=headless_flag,
                    max_concurrency=int(args.max_concurrency),
                    slow_mo=int(args.slow_mo),
                    retries=int(args.retries),
                    timeout_ms=int(args.timeout_ms),
                    block_resources=block_flag,
                    debug=debug_flag,
                )
                await scraper.start()
                try:
                    tn_list = [tn for _, tn in items]
                    logging.info("[scrape-to-csv] Scraping first pass: %d items", len(tn_list))
                    results = await scraper.get_status_many(tn_list, rps=float(args.rps))
                    status_by_tn = {tn: (raw or "").strip() for tn, raw in results}

                    missing = [tn for tn in tn_list if not status_by_tn.get(tn)]
                    logging.info("[scrape-to-csv] Missing after pass1: %d", len(missing))
                    if missing:
                        results2 = await scraper.get_status_many(missing, rps=float(args.rps))
                        for tn, raw in results2:
                            if raw:
                                status_by_tn[tn] = raw.strip()

                    # Write CSV
                    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
                    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(["row", "tracking", "status_raw", "status_normalized"])
                        for row_idx, tn in items:
                            raw = status_by_tn.get(tn, "")
                            norm = TrackerService.normalize_status(raw) if raw else ""
                            writer.writerow([row_idx, tn, raw, norm])
                    logging.info("[scrape-to-csv] CSV written: %s", args.out_csv)
                finally:
                    await scraper.close()

            asyncio.run(_run())
            print(args.out_csv)
            return 0

        if args.command == "compare":
            diffs = compare_statuses(
                sheets,
                start_row=args.start_row,
                end_row=args.end_row,
                only_mismatches=args.only_mismatches,
            )
            logging.info("Compare finished. Differences: %d", len(diffs))
            return 0

        if args.command == "report":
            name = generate_daily_report(
                sheets,
                start_row=args.start_row,
                end_row=args.end_row,
                only_mismatches=args.only_mismatches,
                prefix=getattr(args, "prefix", None),
            )
            logging.info("Report finished. Sheet: %s", name)
            return 0

        if args.command == "mark-compare":
            written = mark_compare_column(
                sheets,
                start_row=args.start_row,
                end_row=args.end_row,
            )
            logging.info("Mark-compare finished. Rows written: %d", written)
            return 0

        if args.command == "all":
            asyncio.run(update_statuses_linux(
                sheets,
                headless=True,
                start_row=args.start_row,
                end_row=args.end_row,
                only_empty=args.only_empty,
                max_concurrency=args.max_concurrency,
                rps=args.rps,
                retries=args.retries,
                timeout_ms=args.timeout_ms,
                batch_size=args.batch_size,
                sleep_between_batches=args.sleep_between_batches,
            ))
            name = generate_daily_report(
                sheets,
                start_row=args.start_row,
                end_row=args.end_row,
                only_mismatches=args.only_mismatches,
                prefix=getattr(args, "prefix", None),
            )
            logging.info("All finished. Report sheet: %s", name)
            return 0

        parser.print_help()
        return 2
    except Exception as e:
        logging.exception("Fatal error in Linux runner: %s", e)
        return 2
    finally:
        logging.info("Linux runner finished")

if __name__ == "__main__":
    raise SystemExit(main())
