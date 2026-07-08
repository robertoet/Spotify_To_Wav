from __future__ import annotations

import logging
import os
import sys
from typing import TextIO


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
RESET = "\033[0m"
LEVEL_COLORS = {
    logging.WARNING: "\033[96m",
    logging.ERROR: "\033[91m",
    logging.CRITICAL: "\033[91m",
}


class ColorFormatter(logging.Formatter):
    def __init__(self, *, use_color: bool) -> None:
        super().__init__(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        if not self.use_color:
            return message

        color = LEVEL_COLORS.get(record.levelno)
        if color is None:
            return message

        return f"{color}{message}{RESET}"


def configure_logger(logger: logging.Logger, stream: TextIO | None = None) -> None:
    if logger.handlers:
        return

    output = stream or sys.stdout
    use_color = output.isatty() and "NO_COLOR" not in os.environ
    handler = logging.StreamHandler(output)
    handler.setFormatter(ColorFormatter(use_color=use_color))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
