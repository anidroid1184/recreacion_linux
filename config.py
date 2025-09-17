import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Candidates for optional .env loading (do not override already-exported vars)
_DOTENV_CANDIDATES = [
    "/app/.env",
    "/app/recreacion_linux/.env",
]
for _p in _DOTENV_CANDIDATES:
    if os.path.isfile(_p) and os.access(_p, os.R_OK):
        load_dotenv(_p, override=False)
        break


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

    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", "/app/recreacion_linux/credentials.json"
    )
    INTER_MAP_PATH: str = os.getenv(
        "INTER_MAP_PATH", "/app/recreacion_linux/interrapidisimo_traking_map.json"
    )


settings = Settings()
