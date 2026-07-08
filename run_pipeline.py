#!/usr/bin/env python3

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from src import csv_clean_and_concat
from src import list_dl_yt
from src.logging_utils import configure_logger


PROJECT_ROOT = Path(__file__).resolve().parent
LOGGER_NAME = "spotify_to_wav.run_pipeline"
logger = logging.getLogger(LOGGER_NAME)


def configure_logging() -> None:
    configure_logger(logger, stream=sys.stdout)


def format_duration(seconds: float) -> str:
    return f"{seconds:.2f}s"


def run_csv_step(input_dir: Path, output_csv: Path) -> int:
    logger.info("Roh-CSV-Ordner: %s", input_dir)
    logger.info("Erzeuge: %s", output_csv)

    try:
        rows = csv_clean_and_concat.collect_rows(input_dir)
        csv_clean_and_concat.write_rows(rows, output_csv)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Fehler beim CSV-Schritt: %s", exc)
        return 1

    logger.info("%s Zeilen gespeichert in '%s'.", len(rows), output_csv)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Führt die Pipeline aus: "
            "1) CSVs zusammenführen, 2) YouTube suchen und Audio herunterladen."
        )
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=None,
        help="Zielordner für die heruntergeladenen Audiodateien (Standard: downloads)",
    )
    parser.add_argument(
        "--input-dir",
        dest="input_dir",
        default="roh",
        help="Pfad zum Ordner mit den Roh-CSV-Dateien (Standard: roh)",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir_option",
        metavar="OUTPUT_DIR",
        default=None,
        help="Zielordner für die heruntergeladenen Audiodateien (Standard: downloads)",
    )
    parser.add_argument(
        "--audio-format",
        dest="audio_format",
        default="wav",
        choices=["wav", "mp3", "m4a", "flac", "opus", "vorbis"],
        help="Zielformat für Audio (Standard: wav)",
    )
    parser.add_argument(
        "--save-links",
        action="store_true",
        help="Speichert zusätzlich eine links.txt im Zielordner",
    )
    parser.add_argument(
        "--cookies-from-browser",
        dest="cookies_from_browser",
        default=None,
        help="Browser für yt-dlp-Cookies, z. B. chrome, firefox, brave, edge",
    )
    parser.add_argument(
        "--no-check-certificates",
        action="store_true",
        help=(
            "Gibt yt-dlp --no-check-certificates weiter. "
            "Nur verwenden, wenn lokale SSL-Zertifikate fehlschlagen."
        ),
    )
    args = parser.parse_args()
    if args.output_dir and args.output_dir_option:
        parser.error("Bitte entweder output_dir oder --output-dir verwenden, nicht beides.")

    args.output_dir = args.output_dir_option or args.output_dir or "downloads"
    return args


def main() -> int:
    configure_logging()
    pipeline_start = time.perf_counter()
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    input_csv = PROJECT_ROOT / "song_namen.csv"
    output_dir = Path(args.output_dir).resolve()

    result = run_csv_step(input_dir=input_dir, output_csv=input_csv)
    if result != 0:
        logger.error("Pipeline fehlgeschlagen (Exit-Code %s).", result)
        return result

    result = list_dl_yt.run_download_pipeline(
        input_csv=input_csv,
        output_dir=output_dir,
        audio_format=args.audio_format,
        save_links=args.save_links,
        cookies_from_browser=args.cookies_from_browser,
        no_check_certificates=args.no_check_certificates,
    )
    if result != 0:
        logger.error("Pipeline fehlgeschlagen (Exit-Code %s).", result)
        return result

    total_duration = time.perf_counter() - pipeline_start
    logger.info("Pipeline abgeschlossen.")
    logger.info("Gesamtzeit Pipeline: %s", format_duration(total_duration))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
