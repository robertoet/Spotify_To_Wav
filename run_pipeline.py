#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from src import list_dl_yt


PROJECT_ROOT = Path(__file__).resolve().parent


def run_step(script: Path, *extra_args: str) -> None:
    command = [sys.executable, str(script), *extra_args]
    print(f"==> Starte: {' '.join(command)}")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Führt die Pipeline aus: "
            "1) CSVs zusammenführen, 2) YouTube suchen und Audio herunterladen."
        )
    )
    parser.add_argument(
        "--input-csv",
        dest="input_csv",
        default=None,
        help="Pfad zur Eingabe-CSV für den Download-Schritt (Standard: song_namen.csv im Projekt-Root)",
    )
    parser.add_argument(
        "output_dir",
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
    input_csv = Path(args.input_csv).resolve() if args.input_csv else (PROJECT_ROOT / "song_namen.csv")
    output_dir = Path(args.output_dir).resolve()

    try:
        run_step(PROJECT_ROOT / "src" / "csv_clean_and_concat.py")
        result = list_dl_yt.run_download_pipeline(
            input_csv=input_csv,
            output_dir=output_dir,
            audio_format=args.audio_format,
            save_links=args.save_links,
            cookies_from_browser=args.cookies_from_browser,
        )
    except subprocess.CalledProcessError as err:
        print(f"Pipeline fehlgeschlagen (Exit-Code {err.returncode}).", file=sys.stderr)
        return err.returncode

    if result != 0:
        print(f"Pipeline fehlgeschlagen (Exit-Code {result}).", file=sys.stderr)
        return result

    print("Pipeline abgeschlossen.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
