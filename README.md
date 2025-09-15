# Linux runner (headless + logs only)

This directory contains a Linux-optimized runner for the Interrapidísimo status updater that:

- Logs only to files under `logs/` (no console output).
- Uses Playwright headless Chromium with resource blocking (images, media, fonts, CSS) to reduce RAM/CPU.
- Defaults are tuned for a ~4GB RAM environment: low concurrency, throttled requests, short timeouts.

## Requirements

- Python 3.10+
- System packages for Chromium (Ubuntu/Debian example):
  - `sudo apt-get update`
  - `sudo apt-get install -y libglib2.0-0 libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libdrm2 libx11-xcb1 libxcomposite1 libxdamage1 libxext6 libxfixes3 libxrandr2 libgbm1 libasound2 libxshmfence1 ca-certificates fonts-liberation libxkbcommon0`
- Python dependencies from the project root `requirements.txt`:
  - `pip install -r requirements.txt`
- Playwright browsers:
  - `python -m playwright install chromium`
  - Optionally on Debian/Ubuntu: `python -m playwright install-deps`

## Environment

Create a `.env` file at project root with at least:

```env
SPREADSHEET_NAME=seguimiento
HEADLESS=true
TZ=America/Bogota
# Optional: DRIVE_FOLDER_ID and DRIVE_FOLDER_INDIVIDUAL_FILE not needed for this runner
```

Place your Google Service Account JSON as `credentials.json` at the project root (same folder as `requirements.txt`). This file is gitignored.

## Run

All output goes to `logs/YYYY-MM-DD.log` (no console output).

From the project root, choose one of the subcommands:

### Scrape (Inter status -> write to sheet)

```bash
python -m recreacion_linux.main scrape \
  --start-row 2 \
  --only-empty true \
  --max-concurrency 2 \
  --rps 0.8
```

### Compare (analyze mismatches only)

```bash
python -m recreacion_linux.main compare \
  --start-row 2 \
  --only-mismatches true
```

### Report (write/append daily report worksheet with mismatches)

```bash
python -m recreacion_linux.main report \
  --start-row 2 \
  --only-mismatches true \
  --prefix Informe_
```

### All (scrape then report)

```bash
python -m recreacion_linux.main all \
  --start-row 2 \
  --only-empty true \
  --max-concurrency 2 \
  --rps 0.8 \
  --only-mismatches true \
  --prefix Informe_
```

### Parameters

- `--start-row` (default 2): First row to process (1-based).
- `--end-row` (optional): Last row to process.
- `--only-empty` (default true): Only fill empty `STATUS TRACKING` cells.
- `--max-concurrency` (default 2): Concurrent pages in Playwright.
- `--rps` (default 0.8): Requests per second pacing.
- `--retries` (default 1): Quick retries for empty results.
- `--timeout-ms` (default 25000): Navigation/wait timeout.
- `--batch-size` (default 1500): Number of rows per browser cycle.
- `--sleep-between-batches` (default 15.0): Pause between batches (seconds).

## Notes on resource usage

- Keeping `--max-concurrency` at 1-2 and enabling resource blocking reduces memory spikes.
- `--rps` throttling prevents bursts that can lead to page timeouts and high CPU.
- Batching keeps the browser fresh without restarting too often.

## What this runner does NOT do

- It does not download new source data from Drive or update the tracking sheet with new rows. It only fills/updates the `STATUS TRACKING` column using the async scraper.
- It does not print to console; check the `logs/` directory for progress and errors.
