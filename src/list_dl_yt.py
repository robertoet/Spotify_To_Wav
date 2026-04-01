import csv
import logging
import subprocess
import sys
import shutil
import re
import time
from pathlib import Path


LOGGER_NAME = "spotify_to_wav.list_dl_yt"
logger = logging.getLogger(LOGGER_NAME)


def configure_logging() -> None:
    if logger.handlers:
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def format_duration(seconds: float) -> str:
    return f"{seconds:.2f}s"


def ensure_ytdlp_exists() -> None:
    if shutil.which("yt-dlp") is None:
        raise FileNotFoundError("yt-dlp wurde nicht gefunden. Installiere es zuerst.")


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
        "--remote-components", "ejs:github",
        f"ytsearch1:{query}",
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
            logger.error("[Search-Fehler] %s", stderr.strip())
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
        "--remote-components", "ejs:github",
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
        for line in stdout.splitlines():
            logger.info("[yt-dlp] %s", line)
    if stderr.strip():
        for line in stderr.splitlines():
            logger.warning("[yt-dlp] %s", line)

    return ok


def process_csv(
    input_csv: Path,
    output_dir: Path,
    links_txt: Path | None = None,
    audio_format: str = "wav",
    cookies_from_browser: str | None = None,
) -> None:
    if not input_csv.exists():
        logger.error("Fehler: Datei nicht gefunden: %s", input_csv)
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    total_start = time.perf_counter()

    # Cache vermeidet doppelte Suchanfragen (schneller, aber nicht aggressiver).
    search_cache: dict[str, str | None] = {}
    rows_processed = 0
    rows_empty = 0
    songs_downloaded = 0
    songs_failed = 0
    songs_no_match = 0
    songs_existing = 0

    with input_csv.open("r", encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.DictReader(csvfile)

        required_columns = {"Artist", "Song", "Album"}
        missing = required_columns - set(reader.fieldnames or [])
        if missing:
            logger.error(
                "Fehler in %s: CSV braucht diese Spalten: Artist,Song,Album. Fehlend: %s",
                input_csv.name,
                ", ".join(sorted(missing)),
            )
            return

        link_file_handle = None
        try:
            if links_txt is not None:
                link_file_handle = links_txt.open("w", encoding="utf-8")

            for row_num, row in enumerate(reader, start=2):
                row_start = time.perf_counter()
                row_status = "UNBEKANNT"
                artist = (row.get("Artist") or "").strip()
                title = (row.get("Song") or "").strip()
                album = (row.get("Album") or "").strip()
                song_label = " - ".join(part for part in [artist, title] if part).strip() or "(leer)"

                if not artist and not title and not album:
                    row_status = "LEER_UEBERSPRUNGEN"
                    rows_empty += 1
                    logger.info("%s | Zeile %s: leer, übersprungen", input_csv.name, row_num)
                    if link_file_handle:
                        link_file_handle.write("KEIN_TREFFER\n")
                else:
                    queries = build_queries(artist, title, album)
                    output_name_base = sanitize_filename(f"{artist} - {title}")
                    final_ext = expected_ext(audio_format)
                    target_file = output_dir / f"{output_name_base}.{final_ext}"

                    if target_file.exists():
                        row_status = "DATEI_BEREITS_VORHANDEN"
                        songs_existing += 1
                        logger.info(
                            "%s | Zeile %s: Datei existiert bereits, übersprungen: %s",
                            input_csv.name,
                            row_num,
                            target_file.name,
                        )
                        if link_file_handle:
                            link_file_handle.write("DATEI_BEREITS_VORHANDEN\n")
                    else:
                        url = None

                        for query in queries:
                            if query in search_cache:
                                url = search_cache[query]
                                if url:
                                    logger.info(
                                        "%s | Zeile %s: Cache-Treffer für: %s",
                                        input_csv.name,
                                        row_num,
                                        query,
                                    )
                                else:
                                    logger.info(
                                        "%s | Zeile %s: Cache (kein Treffer) für: %s",
                                        input_csv.name,
                                        row_num,
                                        query,
                                    )
                            else:
                                logger.info("%s | Zeile %s: Suche nach: %s", input_csv.name, row_num, query)
                                url = search_youtube_first_result(query, cookies_from_browser=cookies_from_browser)
                                search_cache[query] = url
                                if not url:
                                    time.sleep(1)

                            if url:
                                break

                        if not url:
                            row_status = "KEIN_TREFFER"
                            songs_no_match += 1
                            logger.info("%s | Zeile %s: kein Treffer", input_csv.name, row_num)
                            if link_file_handle:
                                link_file_handle.write("KEIN_TREFFER\n")
                        else:
                            logger.info("%s | Zeile %s: Treffer: %s", input_csv.name, row_num, url)
                            if link_file_handle:
                                link_file_handle.write(url + "\n")

                            logger.info("%s | Zeile %s: Download startet ...", input_csv.name, row_num)
                            success = download_audio(
                                url,
                                output_dir,
                                output_name_base=output_name_base,
                                audio_format=audio_format,
                                cookies_from_browser=cookies_from_browser,
                            )

                            if success:
                                row_status = "DOWNLOAD_OK"
                                songs_downloaded += 1
                                logger.info(
                                    "%s | Zeile %s: Download erfolgreich: %s",
                                    input_csv.name,
                                    row_num,
                                    target_file.name,
                                )
                            else:
                                row_status = "DOWNLOAD_FEHLER"
                                songs_failed += 1
                                logger.warning("%s | Zeile %s: Fehler beim Download", input_csv.name, row_num)

                            # Kurze Pause zwischen Downloads als Botting-Schutz.
                            time.sleep(2)

                row_duration = time.perf_counter() - row_start
                rows_processed += 1
                logger.info(
                    "%s | Zeile %s | Song: %s | Status: %s | Bearbeitungszeit: %s",
                    input_csv.name,
                    row_num,
                    song_label,
                    row_status,
                    format_duration(row_duration),
                )

        finally:
            if link_file_handle:
                link_file_handle.close()

    total_duration = time.perf_counter() - total_start
    logger.info("=" * 80)
    logger.info(
        "Zusammenfassung %s | Zeilen: %s | Downloads OK: %s | Bereits vorhanden: %s | Kein Treffer: %s | Download-Fehler: %s | Leer: %s",
        input_csv.name,
        rows_processed,
        songs_downloaded,
        songs_existing,
        songs_no_match,
        songs_failed,
        rows_empty,
    )
    logger.info("Gesamtzeit Song-Verarbeitung: %s", format_duration(total_duration))
    logger.info("=" * 80)


def run_download_pipeline(
    input_csv: Path,
    output_dir: Path,
    audio_format: str = "wav",
    save_links: bool = False,
    cookies_from_browser: str | None = None,
) -> int:
    configure_logging()
    try:
        ensure_ytdlp_exists()
    except FileNotFoundError as exc:
        logger.error("Fehler: %s", exc)
        return 1

    input_csv = input_csv.resolve()
    output_dir = output_dir.resolve()

    if not input_csv.exists() or not input_csv.is_file():
        logger.error("Fehler: Eingabe-CSV nicht gefunden: %s", input_csv)
        return 1

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Eingabe-CSV: %s", input_csv)
    logger.info("Zielordner: %s", output_dir)

    links_txt = output_dir / "links.txt" if save_links else None

    logger.info("=" * 80)
    logger.info("Verarbeite CSV: %s", input_csv.name)
    logger.info("Ausgabeordner: %s", output_dir)
    logger.info("=" * 80)

    process_csv(
        input_csv=input_csv,
        output_dir=output_dir,
        links_txt=links_txt,
        audio_format=audio_format,
        cookies_from_browser=cookies_from_browser,
    )
    return 0


if __name__ == "__main__":
    configure_logging()
    logger.error("Bitte run_pipeline.py verwenden.")
    raise SystemExit(1)
