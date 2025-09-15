import logging
import os
from datetime import datetime


def setup_file_logging(log_dir: str = "logs", level: int = logging.INFO) -> str:
    """Configure logging to write only to a daily rotating file (no console).

    Returns the path of the active log file.
    """
    os.makedirs(log_dir, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    logfile = os.path.join(log_dir, f"{today}.log")

    # Remove existing handlers to avoid duplicate logs if reconfigured
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(logfile, encoding="utf-8"),
        ],
    )

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)
    return logfile
