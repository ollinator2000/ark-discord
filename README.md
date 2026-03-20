# ARK Ascended Discord Gamelog Bot (Windows 10 / Ubuntu)

Bot für **ARK: Survival Ascended PvPvE**, der dein Gamelog live parst und die wichtigsten Events als optisch aufbereitete Discord-Embeds in einen Channel schreibt.

## Features

- Live-Parsing des ARK-Gamelogs (Tail-Modus)
- PvP- und PvE-Events per Regex-Regeln
- Eye-Candy Ausgabe mit Emoji, Farben, Feldern, Timestamp
- Regeln anpassbar über `rules.json` (ohne Code-Änderung)
- Duplicate-Schutz für wiederholte Zeilen
- Anti-Flood: Burst-Events werden in Sammel-Embeds gebündelt
- Persistente Statistiken in SQLite (`ark_stats.db`)
- Leaderboards automatisch alle 6 Stunden
- Leaderboards on-demand per Slash-Command `/leaderboard`

## Voraussetzungen

- Windows 10
- Python 3.11+ empfohlen
- Discord Bot Token + Zugriff auf Ziel-Channel

## Installation

### Windows 10

1. Projektordner öffnen.
2. Virtuelle Umgebung erstellen:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
```

3. Abhängigkeiten installieren:

```powershell
pip install -r requirements.txt
```

4. Umgebungsvariablen setzen:

```powershell
copy .env.example .env
```

5. `.env` anpassen:

- `DISCORD_TOKEN`: Bot Token
- `DISCORD_CHANNEL_ID`: Discord Textchannel ID
- `ARK_LOG_PATH`: Voller Pfad zum ASA Logfile
- `ARK_RULES_PATH`: Standard `rules.json`
- `ARK_DB_PATH`: SQLite-Datei für persistente Stats (z. B. `ark_stats.db`)
- `POLL_INTERVAL_SECONDS`: z. B. `1.5`
- `BURST_TOP_ITEMS`: Anzahl Top-Items im Sammel-Embed (z. B. `5`)
- `BURST_MAX_BUFFER_SIZE`: Sofort-Flush bei sehr großem Burst (z. B. `250`)
- `LEADERBOARD_POST_INTERVAL_SECONDS`: Auto-Post-Intervall (Default `21600` = 6h)
- `ARK_LOG_FILE`: Dateipfad für Bot-Logs (Default `ark_discord_bot.log`)
- `ARK_LOG_LEVEL`: Log-Level für Konsole (z. B. `INFO`, `DEBUG`, `WARNING`)
- `ARK_LOG_FILE_LEVEL`: Log-Level für Datei (Standard wie `ARK_LOG_LEVEL`)
- `ARK_LOG_DISCORD_MESSAGES`: Discord-Nachrichten in Datei-Log schreiben (`true`/`false`, Default `true`)
- `ARK_DISCORD_MESSAGE_DEBUG`: Alias für explizite Discord-Nachrichten-Logs (`true`/`false`, Default `true`)

### Ubuntu / Linux

1. Systempakete installieren:

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip
```

2. Projekt klonen und in den Ordner wechseln:

```bash
git clone https://github.com/ollinator2000/ark-discord.git
cd ark-discord
```

3. Virtuelle Umgebung erstellen:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

4. Abhängigkeiten installieren:

```bash
pip install -r requirements.txt
```

5. Konfiguration anlegen:

```bash
cp .env.example .env
```

6. `.env` anpassen (siehe Liste im Windows-Teil).

## Start

Python-Environment pro neuer Shell starten (Windows):

```powershell
.\.venv\Scripts\activate
```

Danach Bot starten:

```powershell
python bot.py
```

Linux Start:

```bash
source .venv/bin/activate
python bot.py
```

## SQLite Initialisierung

Die DB wird automatisch beim ersten Bot-Start erzeugt und initialisiert.

```powershell
python bot.py
```

Optional prüfen, ob die DB-Datei existiert:

```powershell
dir ark_stats.db
```

Linux:

```bash
ls -lh ark_stats.db
```

Optional Tabellen prüfen (wenn `sqlite3` installiert ist):

```powershell
sqlite3 ark_stats.db ".tables"
```

Linux:

```bash
sqlite3 ark_stats.db ".tables"
```

## Bot-Rechte in Discord

Empfohlen:

- View Channels
- Send Messages
- Embed Links
- Read Message History

## Leaderboard Commands

- `/leaderboard dino_kills`
- `/leaderboard player_kills`
- `/leaderboard dino_tames`
- `/leaderboard all`

### Leaderboard nutzen

So geht es Schritt für Schritt:

1. Bot auf Discord starten und sicherstellen, dass der Slash-Command-Sync durchgelaufen ist.
2. In den Channel klicken, in dem der Bot schreibt.
3. Eingabe:
   - `/leaderboard dino_kills`
   - `/leaderboard player_kills`
   - `/leaderboard dino_tames`
   - `/leaderboard all`
4. Der Bot postet direkt das passende Embed mit Top-5-Werten.

Mögliche Slash-Command Antworten:

- Einzelcommand: exakt 1 Embed
- `all`: bis zu 3 Embeds (für Dino Kills, Player Kills, Dino Tames)

Empfohlene Rechte für Slash-Commands:

- `Send Messages`
- `Embed Links`
- `Use Slash Commands`

Die Rechte sind in der Invite-URL meist enthalten oder in Rollen-Berechtigungen hinterlegt.

## Regeln anpassen (`rules.json`)

Die Erkennung läuft vollständig über Regex-Regeln.
Jede Regel kann Title, Description, Farben und Felder definieren.
Für Anti-Flood kannst du pro Regel zusätzlich setzen:

- `event_class`: `high`, `normal` oder `burst`
- `aggregation_window_seconds`: Sammelfenster für `burst` (z. B. `30`)
- `aggregate_key`: Gruppierung im Sammel-Embed (z. B. `{structure}`)

Beispielstruktur:

```json
{
  "name": "pvp_player_kill",
  "pattern": "(?P<killer>.+?) killed (?P<victim>.+?)$",
  "title": "PvP Kill",
  "description": "**{killer}** hat **{victim}** ausgeschaltet.",
  "emoji": "⚔️",
  "color": 15158332,
  "fields": [
    { "name": "Killer", "value": "{killer}" },
    { "name": "Opfer", "value": "{victim}" }
  ]
}
```

## Wichtiger Hinweis zu ASA Logformat

Logformate unterscheiden sich je nach Server-Setup/Mods. Falls Zeilen nicht erkannt werden:

1. Reale Logzeilen aus deinem `ShooterGame.log` nehmen.
2. `pattern` in `rules.json` darauf anpassen.
3. Bot neu starten.

## Optional: Autostart auf Windows 10

Variante A: Aufgabenplanung (Task Scheduler)

- Trigger: Beim Systemstart
- Aktion: `python` mit Argument `bot.py`
- Starten in: Projektordner

Variante B: NSSM (Non-Sucking Service Manager) als Windows-Service.

## Dateien

- `bot.py`: Bot + Tailer + Parser
- `rules.json`: Event-Regeln
- `.env.example`: Konfig-Vorlage
- `requirements.txt`: Python-Abhängigkeiten
