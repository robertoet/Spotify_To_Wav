import csv
import subprocess
import sys
import shutil
import re
import time
from pathlib import Path
import argparse


def ensure_ytdlp_exists() -> None:
    if shutil.which("yt-dlp") is None:
        print("Fehler: yt-dlp wurde nicht gefunden. Installiere es zuerst.", file=sys.stderr)
        sys.exit(1)


def normalize_text(text: str) -> str:
    text = text.strip()
    text = text.replace(",", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def sanitize_filename(text: str) -> str:
    text = text.strip()
    text = re.sub(r'[<>:"/\\|?*]', "_", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip(" .")
    return text or "unbekannt"


def expected_ext(audio_format: str) -> str:
    if audio_format == "vorbis":
        return "ogg"
    return audio_format


def build_queries(artist: str, title: str, album: str) -> list[str]:
    artist = normalize_text(artist)
    title = normalize_text(title)
    album = normalize_text(album)

    queries = []

    # Beste Suche zuerst: Artist + Title
    q1 = " ".join(part for part in [artist, title] if part).strip()
    if q1:
        queries.append(q1)

    # Dann mit Album
    q2 = " ".join(part for part in [artist, title, album] if part).strip()
    if q2 and q2 not in queries:
        queries.append(q2)

    return queries


def run_cmd(cmd: list[str]) -> tuple[bool, str, str]:
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        return True, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return False, e.stdout or "", e.stderr or ""


def search_youtube_first_result(query: str, cookies_from_browser: str | None = None) -> str | None:
    cmd = [
        "yt-dlp",
        f"ytsearch3:{query}",
        "--print", "webpage_url",
        "--skip-download",
        "--no-warnings",
        "--quiet",
    ]

    if cookies_from_browser:
        cmd.extend(["--cookies-from-browser", cookies_from_browser])

    ok, stdout, stderr = run_cmd(cmd)

    if not ok:
        if stderr.strip():
            print(f"    [Search-Fehler] {stderr.strip()}", file=sys.stderr)
        return None

    urls = [line.strip() for line in stdout.splitlines() if line.strip()]
    return urls[0] if urls else None


def download_audio(
    url: str,
    output_dir: Path,
    output_name_base: str,
    audio_format: str = "wav",
    cookies_from_browser: str | None = None,
) -> bool:
    output_template = output_dir / f"{output_name_base}.%(ext)s"

    cmd = [
        "yt-dlp",
        "-f", "bestaudio/best",
        "--extract-audio",
        "--audio-format", audio_format,
        "--audio-quality", "0",
        "--sleep-requests", "2",
        "--sleep-interval", "3",
        "--max-sleep-interval", "8",
        "--no-overwrites",
        "-o", str(output_template),
        url,
    ]

    if cookies_from_browser:
        cmd.extend(["--cookies-from-browser", cookies_from_browser])

    ok, stdout, stderr = run_cmd(cmd)

    if stdout.strip():
        print(stdout.strip())
    if stderr.strip():
        print(stderr.strip(), file=sys.stderr)

    return ok


def process_csv(
    input_csv: Path,
    output_dir: Path,
    links_txt: Path | None = None,
    audio_format: str = "wav",
    cookies_from_browser: str | None = None,
) -> None:
    if not input_csv.exists():
        print(f"Fehler: Datei nicht gefunden: {input_csv}", file=sys.stderr)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    with input_csv.open("r", encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        required_columns = {"Artist", "Song", "Album"}
        missing = required_columns - set(reader.fieldnames or [])
        if missing:
            print(
                f"Fehler in {input_csv.name}: CSV braucht diese Spalten: Artist,Song,Album. "
                f"Fehlend: {', '.join(sorted(missing))}",
                file=sys.stderr
            )
            return

        link_file_handle = None
        try:
            if links_txt is not None:
                link_file_handle = links_txt.open("w", encoding="utf-8")

            for row_num, row in enumerate(reader, start=2):
                artist = (row.get("Artist") or "").strip()
                title = (row.get("Song") or "").strip()
                album = (row.get("Album") or "").strip()

                if not artist and not title and not album:
                    print(f"{input_csv.name} | Zeile {row_num}: leer, übersprungen")
                    if link_file_handle:
                        link_file_handle.write("KEIN_TREFFER\n")
                    continue

                queries = build_queries(artist, title, album)
                url = None

                for query in queries:
                    print(f"\n{input_csv.name} | Zeile {row_num}: Suche nach: {query}")
                    url = search_youtube_first_result(query, cookies_from_browser=cookies_from_browser)
                    if url:
                        break
                    time.sleep(1)

                if not url:
                    print("  -> kein Treffer")
                    if link_file_handle:
                        link_file_handle.write("KEIN_TREFFER\n")
                    continue

                print(f"  -> Treffer: {url}")
                if link_file_handle:
                    link_file_handle.write(url + "\n")

                output_name_base = sanitize_filename(f"{artist} - {title}")
                final_ext = expected_ext(audio_format)
                target_file = output_dir / f"{output_name_base}.{final_ext}"

                if target_file.exists():
                    print(f"  -> Datei existiert bereits, übersprungen: {target_file.name}")
                    time.sleep(1)
                    continue

                print("  -> Download startet ...")
                success = download_audio(
                    url,
                    output_dir,
                    output_name_base=output_name_base,
                    audio_format=audio_format,
                    cookies_from_browser=cookies_from_browser,
                )

                if success:
                    print(f"  -> Download erfolgreich: {target_file.name}")
                else:
                    print("  -> Fehler beim Download")

                time.sleep(3)

        finally:
            if link_file_handle:
                link_file_handle.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Liest song_namen.csv (oder eine angegebene CSV), sucht YouTube-Treffer "
                    "und lädt die Audiospur mit Dateiname 'Artist - Song.<ext>' herunter."
    )

    parser.add_argument(
        "--input-csv",
        dest="input_csv",
        default=None,
        help="Pfad zur Eingabe-CSV (Standard: song_namen.csv neben dem Script)"
    )

    parser.add_argument(
        "output_dir",
        nargs="?",
        default="downloads",
        help="Zielordner für die heruntergeladenen Audiodateien (Standard: downloads)"
    )

    parser.add_argument(
        "--audio-format",
        dest="audio_format",
        default="wav",
        choices=["wav", "mp3", "m4a", "flac", "opus", "vorbis"],
        help="Zielformat für Audio (Standard: wav)"
    )

    parser.add_argument(
        "--save-links",
        action="store_true",
        help="Speichert zusätzlich eine links.txt im Zielordner"
    )

    parser.add_argument(
        "--cookies-from-browser",
        dest="cookies_from_browser",
        default=None,
        help="Browser für yt-dlp-Cookies, z. B. chrome, firefox, brave, edge"
    )

    return parser.parse_args()


def main():
    ensure_ytdlp_exists()
    args = parse_args()

    script_dir = Path(__file__).resolve().parent
    input_csv = Path(args.input_csv).resolve() if args.input_csv else (script_dir / "song_namen.csv")
    target_root = Path(args.output_dir).resolve()

    if not input_csv.exists() or not input_csv.is_file():
        print(f"Fehler: Eingabe-CSV nicht gefunden: {input_csv}", file=sys.stderr)
        sys.exit(1)

    target_root.mkdir(parents=True, exist_ok=True)

    print(f"Eingabe-CSV: {input_csv}")
    print(f"Zielordner: {target_root}")

    links_txt = target_root / "links.txt" if args.save_links else None

    print("\n" + "=" * 80)
    print(f"Verarbeite CSV: {input_csv.name}")
    print(f"Ausgabeordner: {target_root}")
    print("=" * 80)

    process_csv(
        input_csv=input_csv,
        output_dir=target_root,
        links_txt=links_txt,
        audio_format=args.audio_format,
        cookies_from_browser=args.cookies_from_browser,
    )


if __name__ == "__main__":
    main()
