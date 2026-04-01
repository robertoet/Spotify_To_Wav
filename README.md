# Spotify_To_Wav

Kleines Python-Projekt für den eigenen Gebrauch. 

CSV Dateien der Spotify Playlisten via https://www.chosic.com/spotify-playlist-exporter/ herunterladen und in /roh einfügen.

csv_clean_and_concat.py führt diese zusammen und behält nur noch Author, Titel und Album.

list_dl_yt.py sucht mit Hilfe der csv nach dem gewünschten Lied auf yt und lädt dieses mit yt-dlp herunter. Es wird so gennant wie in der csv Datei.

Ist extra langsam gemacht, weil man sonst Probleme mit Botting bekommen könnte.

## Voraussetzungen

- Python 3.10+
- `ffmpeg` im System installiert (für Audio-Konvertierung durch `yt-dlp`)

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Nutzung

### CSV verarbeiten

```bash
python csv_clean_and_concat.py
```

### Komplette Pipeline

```bash
python run_pipeline.py
```

Optional mit den bisherigen Download-Argumenten:

```bash
python run_pipeline.py [output_dir] \
  --input-dir /pfad/zu/roh_csvs \
  --audio-format wav \
  --save-links \
  --cookies-from-browser chrome
```

## Haftungsausschluss (Disclaimer)

Dieses GitHub-Projekt wird „wie besehen“ („as is“) und ohne jegliche ausdrückliche oder stillschweigende Gewährleistung bereitgestellt. Der Autor übernimmt keine Garantie für die Richtigkeit, Vollständigkeit, Zuverlässigkeit oder Eignung der bereitgestellten Inhalte, des Codes oder der Dokumentation.

Die Nutzung der Software erfolgt ausschließlich auf eigene Verantwortung. Der Autor haftet in keinem Fall für direkte oder indirekte Schäden, einschließlich, aber nicht beschränkt auf Datenverlust, Systemausfälle, entgangenen Gewinn oder sonstige wirtschaftliche Schäden, die aus der Nutzung oder der Unmöglichkeit der Nutzung dieses Projekts entstehen.

Es wird keine Gewähr dafür übernommen, dass das Projekt frei von Fehlern ist oder ohne Unterbrechung funktioniert. Ebenso besteht keine Verpflichtung zur Wartung, Aktualisierung oder Weiterentwicklung des Projekts.

Beiträge von Dritten spiegeln nicht zwingend die Meinung des Autors wider. Für externe Links oder Inhalte Dritter wird ebenfalls keine Haftung übernommen.

Durch die Nutzung dieses Projekts erklärst du dich mit diesen Bedingungen einverstanden.
