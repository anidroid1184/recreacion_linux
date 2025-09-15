from __future__ import annotations
import logging
from typing import Any, List

from recreacion_linux.config import settings
from recreacion_linux.services.sheets_client import SheetsClient
from recreacion_linux.comparer import compare_statuses


def generate_daily_report(
    sheets: SheetsClient,
    start_row: int = 2,
    end_row: int | None = None,
    only_mismatches: bool = True,
    prefix: str | None = None,
) -> str:
    """Generate or append a daily report worksheet with comparison results.

    Returns the worksheet name used.
    """
    diffs: List[List[Any]] = compare_statuses(
        sheets, start_row=start_row, end_row=end_row, only_mismatches=only_mismatches
    )
    if not diffs:
        logging.info("Report: no rows to write (no differences found)")
        # still return expected name for the day for consistency
        return sheets.create_or_append_daily_report([], prefix=(prefix or settings.daily_report_prefix))

    sheet_name = sheets.create_or_append_daily_report(
        diffs,
        prefix=(prefix or settings.daily_report_prefix),
    )
    logging.info("Report written: %s, rows=%d", sheet_name, len(diffs))
    return sheet_name
