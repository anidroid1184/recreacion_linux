from __future__ import annotations
import logging
from datetime import datetime
from typing import Any, List, Tuple

from services.sheets_client import SheetsClient
from services.tracker_service import TrackerService


def compare_statuses(
    sheets: SheetsClient,
    start_row: int = 2,
    end_row: int | None = None,
    only_mismatches: bool = True,
) -> List[List[Any]]:
    """Compare normalized DROPi vs normalized WEB statuses.

    Returns a list of rows suitable for reporting:
    [ID TRACKING, STATUS DROPI (norm), STATUS TRACKING (norm), FECHA VERIFICACIÓN]
    """
    records = sheets.read_main_records_resilient()
    headers = sheets.read_headers()
    total_records = len(records)
    logging.info("Comparer: read %d records from sheet; start_row=%s end_row=%s only_mismatches=%s",
                 total_records, start_row, end_row, only_mismatches)

    required_headers = ["ID DROPI", "ID TRACKING", "STATUS DROPI", "STATUS TRACKING", "Alerta"]
    sheets.ensure_headers(required_headers)

    differences: List[List[Any]] = []

    processed = 0
    last_with_tracking = start_row - 1
    for idx, record in enumerate(records, start=2):
        if idx < start_row:
            continue
        if end_row is not None and idx > end_row:
            break

        tn = str(record.get("ID TRACKING", "")).strip()
        if not tn:
            continue
        last_with_tracking = idx

        dropi_raw = str(record.get("STATUS DROPI", "")).strip()
        web_raw = str(record.get("STATUS TRACKING", "")).strip()

        dropi = TrackerService.normalize_status(dropi_raw)
        web = TrackerService.normalize_status(web_raw) if web_raw else ""

        if only_mismatches and dropi == web:
            continue

        differences.append([
            tn,
            dropi,
            web,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ])
        processed += 1
        if processed % 200 == 0:
            logging.info("Comparer progress: %d mismatches", processed)

    logging.info("Comparer scan finished: processed_rows=%d last_with_tracking_row=%d total_records=%d",
                 max(0, last_with_tracking - start_row + 1), last_with_tracking, total_records)
    logging.info("Comparer done. Total mismatches: %d", len(differences))
    return differences
