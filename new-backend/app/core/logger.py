import logging
import sys
from pathlib import Path

from ..config import settings


def setup_logging():
    """Configure application logging"""

    # Create logs directory if it doesn't exist
    log_file_path = Path(settings.log_file)
    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Create formatters
    detailed_formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    simple_formatter = logging.Formatter(
        "%(levelname)-8s | %(message)s"
    )

    # File handler - detailed logs
    file_handler = logging.FileHandler(settings.log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)

    # Console handler - simple logs
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, settings.log_level.upper()))
    console_handler.setFormatter(simple_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Silence noisy loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)

    logging.info(f"Logging configured: level={settings.log_level}, file={settings.log_file}")


# Filter for hiding "HTTP Request:" logs from httpx
class NoHttpRequestFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not record.getMessage().startswith("HTTP Request:")


# Apply filter to httpx logger
logging.getLogger("httpx").addFilter(NoHttpRequestFilter())
