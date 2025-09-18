import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


# Cargar .env sólo si existe y es legible (sin lanzar PermissionError)
def _load_dotenv_if_readable(path: str) -> None:
    try:
        if os.path.isfile(path) and os.access(path, os.R_OK):
            from dotenv import load_dotenv
            load_dotenv(path, override=False)
    except Exception:
        # Silencioso: nunca romper por .env
        pass


# En Docker, estos son los dos lugares posibles
_load_dotenv_if_readable("/app/.env")
_load_dotenv_if_readable("/app/recreacion_linux/.env")


@dataclass
class Settings:
    # Flags de ejecución
    HEADLESS: bool = _bool("HEADLESS", True)
    DEBUG_SCRAPER: bool = _bool("DEBUG_SCRAPER", False)
    BLOCK_RESOURCES: bool = _bool("BLOCK_RESOURCES", True)

    # Timing
    SLOW_MO: int = _int("SLOW_MO", 100)        # ms
    TIMEOUT_MS: int = _int("TIMEOUT_MS", 120000)

    # Rutas (por defecto dentro del contenedor)
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "/app/recreacion_linux/credentials.json",
    )
    INTER_MAP_PATH: str = os.getenv(
        "INTER_MAP_PATH",
        "/app/recreacion_linux/interrapidisimo_traking_map.json",
    )

    # Proxy opcional
    PROXY_SERVER: str = os.getenv("PROXY_SERVER", "")
    PROXY_USERNAME: str = os.getenv("PROXY_USERNAME", "")
    PROXY_PASSWORD: str = os.getenv("PROXY_PASSWORD", "")


settings = Settings()

import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


# Cargar .env sólo si existe y es legible (sin lanzar PermissionError)
def _load_dotenv_if_readable(path: str) -> None:
    try:
        if os.path.isfile(path) and os.access(path, os.R_OK):
            from dotenv import load_dotenv
            load_dotenv(path, override=False)
    except Exception:
        # Silencioso: nunca romper por .env
        pass


# En Docker, estos son los dos lugares posibles
_load_dotenv_if_readable("/app/.env")
_load_dotenv_if_readable("/app/recreacion_linux/.env")


@dataclass
class Settings:
    # Flags de ejecución
    HEADLESS: bool = _bool("HEADLESS", True)
    DEBUG_SCRAPER: bool = _bool("DEBUG_SCRAPER", False)
    BLOCK_RESOURCES: bool = _bool("BLOCK_RESOURCES", True)

    # Timing
    SLOW_MO: int = _int("SLOW_MO", 100)        # ms
    TIMEOUT_MS: int = _int("TIMEOUT_MS", 120000)

    # Rutas (por defecto dentro del contenedor)
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "/app/recreacion_linux/credentials.json",
    )
    INTER_MAP_PATH: str = os.getenv(
        "INTER_MAP_PATH",
        "/app/recreacion_linux/interrapidisimo_traking_map.json",
    )

    # Proxy opcional
    PROXY_SERVER: str = os.getenv("PROXY_SERVER", "")
    PROXY_USERNAME: str = os.getenv("PROXY_USERNAME", "")
    PROXY_PASSWORD: str = os.getenv("PROXY_PASSWORD", "")


settings = Settings()
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

    # Timing defaults (overridable via CLI flags where applicable)
    SLOW_MO: int = int(os.getenv("SLOW_MO", "100") or 100)
    TIMEOUT_MS: int = int(os.getenv("TIMEOUT_MS", "120000") or 120000)

    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", "/app/recreacion_linux/credentials.json"
    )
    INTER_MAP_PATH: str = os.getenv(
        "INTER_MAP_PATH", "/app/recreacion_linux/interrapidisimo_traking_map.json"
    )

    # Optional proxy settings
    PROXY_SERVER: str | None = os.getenv("PROXY_SERVER")
    PROXY_USERNAME: str | None = os.getenv("PROXY_USERNAME")
    PROXY_PASSWORD: str | None = os.getenv("PROXY_PASSWORD")


settings = Settings()
