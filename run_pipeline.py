#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src import csv_clean_and_concat
from src import list_dl_yt


PROJECT_ROOT = Path(__file__).resolve().parent


def run_csv_step(input_dir: Path, output_csv: Path) -> int:
    print(f"Roh-CSV-Ordner: {input_dir}")
    print(f"Erzeuge: {output_csv}")

    try:
        rows = csv_clean_and_concat.collect_rows(input_dir)
        csv_clean_and_concat.write_rows(rows, output_csv)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Fehler beim CSV-Schritt: {exc}", file=sys.stderr)
        return 1

    print(f"{len(rows)} Zeilen gespeichert in '{output_csv}'.")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Führt die Pipeline aus: "
            "1) CSVs zusammenführen, 2) YouTube suchen und Audio herunterladen."
        )
    )
    parser.add_argument(
        "--input-dir",
        dest="input_dir",
        default="roh",
        help="Pfad zum Ordner mit den Roh-CSV-Dateien (Standard: roh)",
    )
    parser.add_argument(
        "--output-dir",
        nargs="?",
        default="downloads",
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_dir = Path(args.input_dir).resolve()
    input_csv = PROJECT_ROOT / "song_namen.csv"
    output_dir = Path(args.output_dir).resolve()

    result = run_csv_step(input_dir=input_dir, output_csv=input_csv)
    if result != 0:
        print(f"Pipeline fehlgeschlagen (Exit-Code {result}).", file=sys.stderr)
        return result

    result = list_dl_yt.run_download_pipeline(
        input_csv=input_csv,
        output_dir=output_dir,
        audio_format=args.audio_format,
        save_links=args.save_links,
        cookies_from_browser=args.cookies_from_browser,
    )
    if result != 0:
        print(f"Pipeline fehlgeschlagen (Exit-Code {result}).", file=sys.stderr)
        return result

    print("Pipeline abgeschlossen.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
