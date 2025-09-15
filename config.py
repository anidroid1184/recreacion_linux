from dataclasses import dataclass
import os
from dotenv import load_dotenv

# Load environment variables from .env at project root
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))


@dataclass(frozen=True)
class Settings:
    """Centralized runtime configuration for the Linux runner.

    Do not store secrets here. Only IDs, names and flags. Credentials are read
    from credentials.json managed outside of git.
    """
    drive_folder_id: str = os.getenv("DRIVE_FOLDER_ID", "")
    spreadsheet_name: str = os.getenv("SPREADSHEET_NAME", "seguimiento")
    headless: bool = os.getenv("HEADLESS", "true").lower() == "true"
    timezone: str = os.getenv("TZ", "America/Bogota")
    daily_report_prefix: str = os.getenv("DAILY_REPORT_PREFIX", "Informe_")
    individual_report_folder_id: str = (
        os.getenv("DRIVE_FOLER_INDIVIDUAL_FILE")
        or os.getenv("DRIVE_FOLDER_INDIVIDUAL_FILE", "")
    )


settings = Settings()
