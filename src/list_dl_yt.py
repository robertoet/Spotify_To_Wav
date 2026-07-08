import csv
import logging
import os
import subprocess
import shutil
import re
import time
from pathlib import Path

from src.logging_utils import configure_logger


LOGGER_NAME = "spotify_to_wav.list_dl_yt"
logger = logging.getLogger(LOGGER_NAME)
SEARCH_RESULT_LIMIT = 5
SSL_CERT_ERROR_MARKERS = (
    "CERTIFICATE_VERIFY_FAILED",
    "certificate verify failed",
    "unable to get local issuer certificate",
)
AGE_RESTRICTED_ERROR_MARKERS = (
    "age-restricted",
    "sign in to confirm your age",
    "confirm your age",
    "inappropriate for some users",
)


class YtDlpInfrastructureError(RuntimeError):
    pass


class YtDlpAgeRestrictedError(RuntimeError):
    pass


def configure_logging() -> None:
    configure_logger(logger)


def format_duration(seconds: float) -> str:
    return f"{seconds:.2f}s"


def ensure_ytdlp_exists() -> None:
    if shutil.which("yt-dlp") is None:
        raise FileNotFoundError("yt-dlp wurde nicht gefunden. Installiere es zuerst.")


def find_certifi_bundle() -> Path | None:
    try:
        import certifi
    except ImportError:
        return None

    cert_path = Path(certifi.where())
    return cert_path if cert_path.exists() else None


def build_subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    certifi_bundle = find_certifi_bundle()
    if certifi_bundle is None:
        return env

    for key in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE"):
        current_value = env.get(key)
        if not current_value or not Path(current_value).exists():
            env[key] = str(certifi_bundle)

    return env


def build_ytdlp_cmd(no_check_certificates: bool = False) -> list[str]:
    cmd = ["yt-dlp"]
    if no_check_certificates:
        cmd.append("--no-check-certificates")
    cmd.extend(["--remote-components", "ejs:github"])
    return cmd


def is_ssl_certificate_error(stderr: str) -> bool:
    stderr_lower = stderr.lower()
    return any(marker.lower() in stderr_lower for marker in SSL_CERT_ERROR_MARKERS)


def is_age_restricted_error(stderr: str) -> bool:
    stderr_lower = stderr.lower()
    return any(marker in stderr_lower for marker in AGE_RESTRICTED_ERROR_MARKERS)


def ssl_certificate_error_message() -> str:
    return (
        "yt-dlp konnte HTTPS-Zertifikate nicht prüfen. "
        "Installiere/aktualisiere die Abhängigkeiten mit "
        "`pip install -r requirements.txt`. "
        "Falls dein lokaler Zertifikatsspeicher trotzdem defekt ist, "
        "kannst du notfalls `--no-check-certificates` verwenden."
    )


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
            check=True,
            env=build_subprocess_env(),
        )
        return True, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return False, e.stdout or "", e.stderr or ""


def log_ytdlp_stderr(stderr: str, *, command_failed: bool) -> None:
    if not stderr.strip():
        return

    log_method = logger.warning
    if command_failed and not is_age_restricted_error(stderr):
        log_method = logger.error

    for line in stderr.splitlines():
        log_method("[yt-dlp] %s", line)


def parse_youtube_urls(stdout: str) -> list[str]:
    urls = []
    seen = set()
    for line in stdout.splitlines():
        url = line.strip()
        if not url:
            continue
        if "youtube.com/watch?" not in url and "youtu.be/" not in url:
            continue
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def search_youtube_results(
    query: str,
    cookies_from_browser: str | None = None,
    no_check_certificates: bool = False,
) -> list[str]:
    cmd = build_ytdlp_cmd(no_check_certificates=no_check_certificates) + [
        "--flat-playlist",
        f"ytsearch{SEARCH_RESULT_LIMIT}:{query}",
        "--print", "webpage_url",
        "--skip-download",
        "--no-warnings",
        "--quiet",
    ]

    if cookies_from_browser:
        cmd.extend(["--cookies-from-browser", cookies_from_browser])

    ok, stdout, stderr = run_cmd(cmd)
    urls = parse_youtube_urls(stdout)

    if not ok:
        if is_ssl_certificate_error(stderr):
            if stderr.strip():
                logger.error("[Search-Fehler] %s", stderr.strip())
            raise YtDlpInfrastructureError(ssl_certificate_error_message())
        if is_age_restricted_error(stderr):
            if stderr.strip():
                logger.warning("[Search-Hinweis] Altersbeschränkter Treffer übersprungen: %s", stderr.strip())
            return urls
        if stderr.strip():
            logger.error("[Search-Fehler] %s", stderr.strip())
        return urls

    return urls


def search_youtube_first_result(
    query: str,
    cookies_from_browser: str | None = None,
    no_check_certificates: bool = False,
) -> str | None:
    urls = search_youtube_results(
        query,
        cookies_from_browser=cookies_from_browser,
        no_check_certificates=no_check_certificates,
    )
    return urls[0] if urls else None


def download_audio(
    url: str,
    output_dir: Path,
    output_name_base: str,
    audio_format: str = "wav",
    cookies_from_browser: str | None = None,
    no_check_certificates: bool = False,
) -> bool:
    output_template = output_dir / f"{output_name_base}.%(ext)s"

    cmd = build_ytdlp_cmd(no_check_certificates=no_check_certificates) + [
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
    log_ytdlp_stderr(stderr, command_failed=not ok)

    if not ok and is_ssl_certificate_error(stderr):
        raise YtDlpInfrastructureError(ssl_certificate_error_message())
    if not ok and is_age_restricted_error(stderr):
        raise YtDlpAgeRestrictedError("Altersbeschränkter YouTube-Treffer")

    return ok


def process_csv(
    input_csv: Path,
    output_dir: Path,
    links_txt: Path | None = None,
    audio_format: str = "wav",
    cookies_from_browser: str | None = None,
    no_check_certificates: bool = False,
) -> None:
    if not input_csv.exists():
        logger.error("Fehler: Datei nicht gefunden: %s", input_csv)
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    total_start = time.perf_counter()

    # Cache vermeidet doppelte Suchanfragen (schneller, aber nicht aggressiver).
    search_cache: dict[str, list[str]] = {}
    rows_processed = 0
    rows_empty = 0
    songs_downloaded = 0
    songs_failed = 0
    songs_no_match = 0
    songs_existing = 0
    songs_age_restricted = 0

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
                        candidate_urls = []
                        seen_urls = set()

                        for query in queries:
                            if query in search_cache:
                                query_urls = search_cache[query]
                                if query_urls:
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
                                query_urls = search_youtube_results(
                                    query,
                                    cookies_from_browser=cookies_from_browser,
                                    no_check_certificates=no_check_certificates,
                                )
                                search_cache[query] = query_urls
                                if not query_urls:
                                    time.sleep(1)

                            for candidate_url in query_urls:
                                if candidate_url in seen_urls:
                                    continue
                                seen_urls.add(candidate_url)
                                candidate_urls.append(candidate_url)

                            if candidate_urls:
                                break

                        if not candidate_urls:
                            row_status = "KEIN_TREFFER"
                            songs_no_match += 1
                            logger.info("%s | Zeile %s: kein Treffer", input_csv.name, row_num)
                            if link_file_handle:
                                link_file_handle.write("KEIN_TREFFER\n")
                        else:
                            successful_url = None

                            for candidate_index, candidate_url in enumerate(candidate_urls, start=1):
                                logger.info(
                                    "%s | Zeile %s: Treffer %s/%s: %s",
                                    input_csv.name,
                                    row_num,
                                    candidate_index,
                                    len(candidate_urls),
                                    candidate_url,
                                )
                                logger.info("%s | Zeile %s: Download startet ...", input_csv.name, row_num)

                                try:
                                    success = download_audio(
                                        candidate_url,
                                        output_dir,
                                        output_name_base=output_name_base,
                                        audio_format=audio_format,
                                        cookies_from_browser=cookies_from_browser,
                                        no_check_certificates=no_check_certificates,
                                    )
                                except YtDlpAgeRestrictedError:
                                    songs_age_restricted += 1
                                    logger.warning(
                                        "%s | Zeile %s: altersbeschränkter Treffer übersprungen: %s",
                                        input_csv.name,
                                        row_num,
                                        candidate_url,
                                    )
                                    continue

                                if success:
                                    successful_url = candidate_url
                                    break

                                logger.warning(
                                    "%s | Zeile %s: Kandidat fehlgeschlagen, versuche nächsten Treffer: %s",
                                    input_csv.name,
                                    row_num,
                                    candidate_url,
                                )

                            if successful_url:
                                row_status = "DOWNLOAD_OK"
                                songs_downloaded += 1
                                if link_file_handle:
                                    link_file_handle.write(successful_url + "\n")
                                logger.info(
                                    "%s | Zeile %s: Download erfolgreich: %s",
                                    input_csv.name,
                                    row_num,
                                    target_file.name,
                                )
                            else:
                                row_status = "DOWNLOAD_FEHLER"
                                songs_failed += 1
                                logger.warning(
                                    "%s | Zeile %s: kein herunterladbarer Treffer gefunden",
                                    input_csv.name,
                                    row_num,
                                )
                                if link_file_handle:
                                    link_file_handle.write("DOWNLOAD_FEHLER\n")

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
        "Zusammenfassung %s | Zeilen: %s | Downloads OK: %s | Bereits vorhanden: %s | Kein Treffer: %s | Download-Fehler: %s | Altersbeschränkt übersprungen: %s | Leer: %s",
        input_csv.name,
        rows_processed,
        songs_downloaded,
        songs_existing,
        songs_no_match,
        songs_failed,
        songs_age_restricted,
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
    no_check_certificates: bool = False,
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
    certifi_bundle = find_certifi_bundle()
    if certifi_bundle is not None and not no_check_certificates:
        logger.info("CA-Zertifikate: %s", certifi_bundle)
    if no_check_certificates:
        logger.warning("SSL-Zertifikatsprüfung für yt-dlp ist deaktiviert.")

    links_txt = output_dir / "links.txt" if save_links else None

    logger.info("=" * 80)
    logger.info("Verarbeite CSV: %s", input_csv.name)
    logger.info("Ausgabeordner: %s", output_dir)
    logger.info("=" * 80)

    try:
        process_csv(
            input_csv=input_csv,
            output_dir=output_dir,
            links_txt=links_txt,
            audio_format=audio_format,
            cookies_from_browser=cookies_from_browser,
            no_check_certificates=no_check_certificates,
        )
    except YtDlpInfrastructureError as exc:
        logger.error("Pipeline abgebrochen: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    configure_logging()
    logger.error("Bitte run_pipeline.py verwenden.")
    raise SystemExit(1)
