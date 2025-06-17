import logging
import sys
import os
from pathlib import Path

# Represents the Parent Directory for the Repository
REPO_ROOT = Path(__file__).resolve().parents[2]

# Initializes the logger
def init_logger(name : str, logfile : str = None, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Check is logger is already initialized
    if logger.hasHandlers():
        return logger

    # File Handler
    if logfile is None:
        script_name = Path(sys.argv[0]).stem
        logfile = f"logs/{script_name}.log"

    full_log_path = REPO_ROOT / logfile
    full_log_path.parent.mkdir(parents=True, exist_ok=True)

    fh = logging.FileHandler(full_log_path)
    fh.setLevel(level)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.ERROR)

    # Formatting the logger
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s : %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

# References the logger for utility Suites
def get_logger(name: str = "root") -> logging.Logger:
    return logging.getLogger(name)
