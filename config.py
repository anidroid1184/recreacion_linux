import os
from dataclasses import dataclass
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOTENV_PATH = os.path.join("/app", ".env")

# Load .env only if it exists and is readable (avoid PermissionError when host mounts restricted)
if os.path.isfile(DOTENV_PATH) and os.access(DOTENV_PATH, os.R_OK):
    load_dotenv(DOTENV_PATH)


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


@dataclass
class Settings:
    HEADLESS: bool = _bool("HEADLESS", True)
    DEBUG_SCRAPER: bool = _bool("DEBUG_SCRAPER", False)
    BLOCK_RESOURCES: bool = _bool("BLOCK_RESOURCES", True)

    # Optional IDs/names
    DRIVE_FOLDER_ID: str = os.getenv("DRIVE_FOLDER_ID", "")
    SPREADSHEET_NAME: str = os.getenv("SPREADSHEET_NAME", "seguimiento")
    TZ: str = os.getenv("TZ", "America/Bogota")
    DAILY_REPORT_PREFIX: str = os.getenv("DAILY_REPORT_PREFIX", "Informe_")

    # Credentials and mapping file paths (defaults inside container)
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", "/app/recreacion_linux/credentials.json"
    )
    INTER_MAP_PATH: str = os.getenv(
        "INTER_MAP_PATH", "/app/recreacion_linux/interrapidisimo_traking_map.json"
    )


settings = Settings()
