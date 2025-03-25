import logging
import os
import sys
from colorama import Fore, Style, init

# Optional Prefect support
try:
    from prefect import get_run_logger

    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False

# Initialize colorama
init(autoreset=True)

# Read log level from environment
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()

# Base logger setup
logger = logging.getLogger('jetshift')
logger.setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))

if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(f"{Fore.GREEN}INFO:{Style.RESET_ALL} %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_logger(name=None):
    """
    Return Prefect run logger if in context, else default logger.
    """
    if PREFECT_AVAILABLE:
        try:
            return get_run_logger()
        except RuntimeError:
            pass
    return logger if name is None else logger.getChild(name)


def set_log_level(level):
    """
    Dynamically set the log level for jetshift_core logger.
    Example: set_log_level(logging.DEBUG)
    """
    logger.setLevel(level)
