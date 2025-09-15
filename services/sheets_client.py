from __future__ import annotations
from typing import List, Dict, Any
import gspread
import logging
from datetime import datetime
import time


class SheetsClient:
    """Encapsulates operations on the tracking spreadsheet using gspread.
    Local copy for recreacion_linux (independent from root project).
    """

    def __init__(self, credentials, spreadsheet_name: str):
        self.gc = gspread.authorize(credentials)
        self.spreadsheet = self.gc.open(spreadsheet_name)

    # --- Main sheet helpers ---
    def sheet(self):
        return self.spreadsheet.sheet1

    def read_main_records(self) -> List[Dict[str, Any]]:
        return self.sheet().get_all_records()

    def read_main_records_resilient(self) -> List[Dict[str, Any]]:
        """Read all rows using get_all_values(), avoiding early stop at the first blank row.

        It builds dict records using the header row (row 1). Trailing missing
        cells are filled as empty strings to keep consistent keys.
        """
        sh = self.sheet()
        all_values = sh.get_all_values()
        if not all_values:
            return []
        headers = [h.strip() for h in (all_values[0] if all_values else [])]
        if not headers:
            return []
        records: List[Dict[str, Any]] = []
        for row in all_values[1:]:
            # Ensure row length matches headers length
            if len(row) < len(headers):
                row = row + [""] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[:len(headers)]
            rec = {headers[i]: row[i] for i in range(len(headers))}
            records.append(rec)
        return records

    def read_headers(self) -> List[str]:
        return self.sheet().row_values(1)

    def ensure_headers(self, headers: List[str]) -> None:
        existing = self.read_headers()
        to_add = []
        for h in headers:
            if h not in existing:
                to_add.append(h)
                existing.append(h)
        if to_add:
            # Add missing headers at the end
            for idx, h in enumerate(to_add, start=len(existing) - len(to_add) + 1):
                self.sheet().update_cell(1, idx, h)
            logging.info("Added missing headers: %s", to_add)

    def append_new_rows(self, rows: List[List[Any]]) -> int:
        if not rows:
            return 0
        sh = self.sheet()
        all_values = sh.get_all_values()
        last_row = len(all_values) + 1
        end_row = last_row + len(rows) - 1
        if end_row > sh.row_count:
            extra = end_row - sh.row_count
            logging.info("Adding %d extra rows to accommodate data", extra)
            sh.add_rows(extra)
        sh.update(f"A{last_row}:E{end_row}", rows)
        return len(rows)

    def update_range(self, a1_range: str, values: List[List[Any]]):
        # Simple retry with backoff to handle 429 rate limit bursts
        delay = 1.0
        for attempt in range(5):
            try:
                self.sheet().update(a1_range, values)
                return
            except Exception as e:
                msg = str(e)
                if "429" in msg or "quota" in msg.lower():
                    logging.warning("Sheets update_range throttled (attempt %d): %s", attempt + 1, msg)
                    time.sleep(delay)
                    delay = min(delay * 2, 30)
                    continue
                raise

    def values_batch_update(self, data: List[Dict[str, Any]], value_input_option: str = "RAW"):
        """Perform a single batch update for many disjoint ranges.

        data items: {"range": A1, "values": [[...], ...]}
        """
        body = {
            "valueInputOption": value_input_option,
            "data": data,
        }
        delay = 1.0
        for attempt in range(5):
            try:
                # gspread exposes Spreadsheet.values_batch_update
                return self.spreadsheet.values_batch_update(body)
            except Exception as e:
                msg = str(e)
                if "429" in msg or "quota" in msg.lower():
                    logging.warning("Sheets values_batch_update throttled (attempt %d): %s", attempt + 1, msg)
                    time.sleep(delay)
                    delay = min(delay * 2, 30)
                    continue
                raise

    # --- Daily report helpers ---
    def create_or_append_daily_report(self, rows: List[List[Any]], prefix: str = "Informe_") -> str:
        if not rows:
            # still return the expected name for consistency
            date_name = datetime.now().strftime("%Y-%m-%d")
            return f"{prefix}{date_name}"
        date_name = datetime.now().strftime("%Y-%m-%d")
        sheet_name = f"{prefix}{date_name}"
        try:
            try:
                ws = self.spreadsheet.worksheet(sheet_name)
            except gspread.WorksheetNotFound:
                ws = self.spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=10)
                headers = ["ID TRACKING", "STATUS DROPI", "STATUS TRACKING", "FECHA VERIFICACIÓN"]
                ws.update("A1:D1", [headers])

            start_row = len(ws.get_all_values()) + 1
            end_row = start_row + len(rows) - 1
            if end_row > ws.row_count:
                ws.add_rows(end_row - ws.row_count)
            ws.update(f"A{start_row}:D{end_row}", rows)
            logging.info("Daily report updated: %s, rows: %d", sheet_name, len(rows))
            return sheet_name
        except Exception as e:
            logging.error("Error updating daily report: %s", e)
            return sheet_name
