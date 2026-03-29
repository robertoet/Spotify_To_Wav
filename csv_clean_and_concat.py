#!/usr/bin/env python3

from __future__ import annotations

import csv
from pathlib import Path


INPUT_DIR = Path("roh")
OUTPUT_FILE = Path("song_namen.csv")
REQUIRED_COLUMNS = ("Song", "Artist", "Album")
OUTPUT_COLUMNS = ("Artist", "Song", "Album")


def collect_rows(input_dir: Path) -> list[dict[str, str]]:
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"Keine CSV-Dateien in '{input_dir}' gefunden.")

    unique_keys: set[tuple[str, str, str]] = set()
    merged_rows: list[dict[str, str]] = []

    for csv_file in csv_files:
        with csv_file.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                continue

            missing = [column for column in REQUIRED_COLUMNS if column not in reader.fieldnames]
            if missing:
                raise ValueError(
                    f"Datei '{csv_file}' fehlt Spalten: {', '.join(missing)}"
                )

            for row in reader:
                cleaned = {column: (row.get(column, "") or "").strip() for column in REQUIRED_COLUMNS}
                key = tuple(cleaned[column] for column in REQUIRED_COLUMNS)
                if key not in unique_keys:
                    unique_keys.add(key)
                    merged_rows.append(cleaned)

    merged_rows.sort(
        key=lambda row: (
            row["Artist"].casefold(),
            row["Song"].casefold(),
            row["Album"].casefold(),
        )
    )

    return merged_rows


def write_rows(rows: list[dict[str, str]], output_file: Path) -> None:
    with output_file.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(OUTPUT_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = collect_rows(INPUT_DIR)
    write_rows(rows, OUTPUT_FILE)
    print(f"{len(rows)} Zeilen gespeichert in '{OUTPUT_FILE}'.")


if __name__ == "__main__":
    main()
