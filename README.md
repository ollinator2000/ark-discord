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
- Optionaler WildDinoKill CSV-Ingest (separater Pfad, getrennt vom ShooterGame-Log)
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
- `DISCORD_GUILD_ID`: Optional für sofortige Slash-Command-Synchronisierung in genau einem Server
- `ARK_LOG_PATH`: Voller Pfad zum ASA Logfile
- `ARK_WILD_KILLS_FEATURE_ENABLED`: `true`/`false` (Default `false`) aktiviert CSV-Ingest fuer Wild-Dino-Kills
- `ARK_WILD_KILLS_CSV_PATH`: Voller Pfad zur `wild_kills.csv` (kann auf anderem Verzeichnis liegen als `ShooterGame.log`)
- `ARK_RULES_PATH`: Standard `rules.json`
- `ARK_DB_PATH`: SQLite-Datei für persistente Stats (z. B. `ark_stats.db`)
- `POLL_INTERVAL_SECONDS`: z. B. `1.5`
- `ARK_LOG_HARD_REOPEN_INTERVAL_SECONDS`: Erzwungener Dateirescan/Neueröffnung in Sekunden (Default `900` = 15 Minuten, `0` = aus)
- `BURST_TOP_ITEMS`: Anzahl Top-Items im Sammel-Embed (z. B. `5`)
- `BURST_MAX_BUFFER_SIZE`: Sofort-Flush bei sehr großem Burst (z. B. `250`)
- `LEADERBOARD_POST_INTERVAL_SECONDS`: Auto-Post-Intervall (Default `21600` = 6h)
- `ARK_DISCORD_POSTING_ENABLED`: `true`/`false` (Default `true`) zum temporären Deaktivieren aller Discord-Posts
- `ARK_DB_DISCORD_LOG_ENABLED`: `true`/`false` (Default `false`) für kompakte DB-Zugriffstelemetrie im Discord-Channel
- `ARK_DB_DISCORD_LOG_INTERVAL_SECONDS`: Intervall für DB-Telemetrie-Posts (Default `300`)
- `ARK_LOG_FILE`: Dateipfad für Bot-Logs (Default `ark_discord_bot.log`)
- `ARK_LOG_LEVEL`: Log-Level für Konsole (z. B. `INFO`, `DEBUG`, `WARNING`)
- `ARK_LOG_FILE_LEVEL`: Log-Level für Datei (Standard wie `ARK_LOG_LEVEL`)
- `ARK_LOG_DISCORD_MESSAGES`: Discord-Nachrichten in Datei-Log schreiben (`true`/`false`, Default `true`)
- `ARK_DISCORD_MESSAGE_DEBUG`: Alias für explizite Discord-Nachrichten-Logs (`true`/`false`, Default `true`)

Beispiel fuer getrennte Pfade:

```env
ARK_LOG_PATH=/home/ark/ShooterGame/Saved/Logs/ShooterGame.log
ARK_WILD_KILLS_FEATURE_ENABLED=true
ARK_WILD_KILLS_CSV_PATH=/srv/ark/plugins/WildDinoKill/wild_kills.csv
```

Hinweis zu Slash-Commands:

- ohne `DISCORD_GUILD_ID` werden Commands global synchronisiert (kann verzögert sichtbar sein)
- mit `DISCORD_GUILD_ID` werden Commands guild-spezifisch synchronisiert (normalerweise sofort nach Bot-Neustart sichtbar)

## WildDinoKill CSV

Wenn `ARK_WILD_KILLS_FEATURE_ENABLED=true` gesetzt ist, liest der Bot die Datei aus `ARK_WILD_KILLS_CSV_PATH` inkrementell ein.

- CSV-Datei kann in einem komplett anderen Verzeichnis liegen als `ShooterGame.log`
- pro Kill werden `killer_name` (Player) und `dino_blueprint` (Dino-Typ) verarbeitet
- die Werte werden in SQLite mitgezählt und fließen in `dino_kills` Leaderboards ein
- der Ingest ist restart-sicher: der Leseposition-Offset wird in SQLite gespeichert
- wiederholte Header-Zeilen in der CSV werden ignoriert

Aktuell sendet der CSV-Ingest keine separaten Live-Event-Embeds. Er schreibt in die Statistik, die für Leaderboards und `/lastkill` verwendet wird.

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

## Logging

Der Bot erzeugt zwei Logging-Ziele:

- Konsolenausgabe (Stdout)
- Datei-Log (standardmäßig `ark_discord_bot.log`)

### Welche Logs gibt es?

- `START`: Bot-Start, Konfiguration, geöffnete Dateien, Task- und Loop-Starts
- `LOGFILE`: Log-Tailing-Verhalten  
  - gefundene Kandidat-Dateien  
  - aktives Logfile  
  - Dateiswitch bei Rotation/Inode-Wechsel  
  - leseleere Ticks / Wechselversuche
- `EVENT`: erkannte Events, Burst-Queues, Cooldowns, Scheduler-Ticks
- `DISCORD`: gesendete Discord-Nachrichten (Payload inkl. Channel, Titel/Description/Fields/Footer)  
  - diese Logs werden mit Marker `DISCORD_MESSAGE` gekennzeichnet
- `DB`: SQLite-Initialisierung, Persistenz-Operationen, Queries
- `ERROR`: Exceptions inklusive Stacktrace

Optional (Discord-Channel):

- kompakte DB-Telemetrie-Nachricht im Format `DB-Telemetrie (300s): reads=.. writes=.. commits=..`
- wird nur gesendet, wenn seit dem letzten Intervall DB-Aktivitaet stattgefunden hat

### Wie konfiguriere ich Logging?

In der `.env` sind die Schalter:

- `ARK_LOG_FILE` (Default: `ark_discord_bot.log`)
  - Dateipfad für das Bot-Logfile
- `ARK_LOG_LEVEL` (Default: `INFO`)
  - Konsole: `DEBUG` / `INFO` / `WARNING` / `ERROR`
- `ARK_LOG_FILE_LEVEL` (Default wie `ARK_LOG_LEVEL`)
  - Dateilog: `DEBUG` / `INFO` / `WARNING` / `ERROR`
- `ARK_LOG_HARD_REOPEN_INTERVAL_SECONDS` (Default: `900`)
  - Intervall in Sekunden, nach dem der Logtail zwangsweise neu verifiziert wird (`0` = aus)
- `ARK_DISCORD_POSTING_ENABLED` (Default: `true`)
  - `false` = keine Discord-Posts (inklusive Event-/Leaderboards), nur Bot-Log + Persistierung
- `ARK_DB_DISCORD_LOG_ENABLED` (Default: `false`)
  - `true` = periodische, kompakte DB-Telemetrie als Discord-Nachricht (nur bei Aktivität)
- `ARK_DB_DISCORD_LOG_INTERVAL_SECONDS` (Default: `300`)
  - Intervall in Sekunden für DB-Telemetrie
- `ARK_LOG_DISCORD_MESSAGES` (Default: `true`)
  - `true` = Discord-Nachrichten auch ins Bot-Log schreiben
- `ARK_DISCORD_MESSAGE_DEBUG` (Default: `true`)
  - zusätzliches Schaltfeld für Event-Post-Logging (zusätzlich zu `ARK_LOG_DISCORD_MESSAGES`)
  - für vollständige Discord-Nachrichtenlogs müssen **beide** Flags `true` sein

Beispiel:

```bash
export ARK_LOG_LEVEL=INFO
export ARK_LOG_FILE_LEVEL=DEBUG
export ARK_LOG_HARD_REOPEN_INTERVAL_SECONDS=900
export ARK_DISCORD_POSTING_ENABLED=true
export ARK_DB_DISCORD_LOG_ENABLED=false
export ARK_DB_DISCORD_LOG_INTERVAL_SECONDS=300
export ARK_LOG_DISCORD_MESSAGES=true
export ARK_DISCORD_MESSAGE_DEBUG=true
```

### Logfile live beobachten

```bash
tail -f ark_discord_bot.log
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

## Datenbankschema (SQLite)

Die Datei `ark_stats.db` speichert folgende Daten:

- Spieler-Stammdaten (Name, first/last seen)
- Tribe-Stammdaten und Player-zu-Tribe Zuordnung
- Aggregierte Stats für Leaderboards
- Event-Historie (Dino-Tames, Player-Kills, Dino-Kills)
- Ingestion-Offsets für CSV-Quellen (z. B. WildDinoKill)

Tabellen im Detail:

- `players`
  - Zweck: eindeutige Spielerliste
  - Wichtige Felder: `id`, `player_name` (UNIQUE, case-insensitive), `first_seen_at`, `last_seen_at`
- `tribes`
  - Zweck: eindeutige Tribeliste
  - Wichtige Felder: `id`, `tribe_name` (UNIQUE, case-insensitive), `first_seen_at`, `last_seen_at`
- `player_tribe_membership`
  - Zweck: Zuordnung Spieler <-> Tribe
  - Wichtige Felder: `player_id`, `tribe_id`, `last_seen_at`
  - Primärschlüssel: `(player_id, tribe_id)`
- `player_stats`
  - Zweck: aggregierte Werte pro Spieler (Basis für Leaderboards)
  - Wichtige Felder: `player_id` (PK), `dino_kills_total`, `player_kills_total`, `dino_tames_total`, `updated_at`
- `player_dino_kills_by_type`
  - Zweck: aggregierte Dino-Kills je Spieler und Dino-Typ
  - Wichtige Felder: `player_id`, `dino_type`, `kills_count`, `updated_at`
  - Primärschlüssel: `(player_id, dino_type)`
- `dino_tame_events`
  - Zweck: Einzel-Events für Tames
  - Wichtige Felder: `id`, `player_id`, `tribe_id`, `dino_type`, `dino_level`, `event_time_text`, `recorded_at`
- `player_kill_events`
  - Zweck: Einzel-Events für Player-Kills
  - Wichtige Felder: `id`, `killer_player_id`, `victim_name`, `victim_player_id`, `event_time_text`, `recorded_at`
- `player_death_events`
  - Zweck: Einzel-Events für Player-Deaths (auch ohne validen Player-Killer)
  - Wichtige Felder: `id`, `victim_player_id`, `victim_name`, `killer_text`, `event_time_text`, `source_rule`, `recorded_at`
- `dino_kill_events`
  - Zweck: Einzel-Events für Dino-Kills (u. a. Basis für `/lastkill`)
  - Wichtige Felder: `id`, `killer_player_id`, `dino_type`, `event_time_text`, `source`, `recorded_at`
- `ingestion_offsets`
  - Zweck: merkt, bis zu welcher Byte-Position externe Quellen bereits eingelesen wurden
  - Wichtige Felder: `source_key` (PK), `position`, `updated_at`

## DB Schnelltests (SQLite)

### Letzten Event-Eintrag finden

Zeigt den neuesten `recorded_at`-Wert aus allen Event-Tabellen:

```bash
sqlite3 ark_stats.db "
SELECT MAX(ts) AS last_event_utc
FROM (
  SELECT MAX(recorded_at) AS ts FROM dino_kill_events
  UNION ALL
  SELECT MAX(recorded_at) AS ts FROM dino_tame_events
  UNION ALL
  SELECT MAX(recorded_at) AS ts FROM player_kill_events
  UNION ALL
  SELECT MAX(recorded_at) AS ts FROM player_death_events
);
"
```

### Letztes CSV-Ingest-Update prüfen

```bash
sqlite3 ark_stats.db "SELECT MAX(updated_at) AS last_ingest_update_utc FROM ingestion_offsets;"
```

### Live prüfen, ob die DB weiter aktualisiert wird

```bash
watch -n 5 "sqlite3 ark_stats.db \"SELECT MAX(updated_at) FROM player_stats;\""
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
- `/lastkill <playername>`

### Leaderboard nutzen

So geht es Schritt für Schritt:

1. Bot auf Discord starten und sicherstellen, dass der Slash-Command-Sync durchgelaufen ist.
2. In den Channel klicken, in dem der Bot schreibt.
3. Eingabe:
   - `/leaderboard dino_kills`
   - `/leaderboard player_kills`
   - `/leaderboard dino_tames`
   - `/leaderboard all`
   - `/lastkill Ollinator`
4. Der Bot postet direkt das passende Embed mit Top-5-Werten.
5. Bei `/lastkill <playername>` postet der Bot den zuletzt gespeicherten Dino-Kill dieses Spielers (inkl. Zeit und Quelle).

Mögliche Slash-Command Antworten:

- Einzelcommand: exakt 1 Embed
- `all`: bis zu 3 Embeds (für Dino Kills, Player Kills, Dino Tames)
- `/lastkill`: 1 Embed mit letztem gespeicherten Dino-Kill (Spieler, Dino, Zeit, Quelle)

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
