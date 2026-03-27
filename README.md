# ARK Ascended Discord Gamelog Bot

Bot fuer **ARK: Survival Ascended PvPvE**, der Logdaten verarbeitet, Events in Discord postet und persistente Statistiken in SQLite fuer Leaderboards fuehrt.

## 1. Ueberblick

Der Bot kombiniert mehrere Datenquellen:

- `ShooterGame.log` fuer Live-Events (Join/Leave, Deaths, Tames, Structure-Events, etc.)
- optional `wild_kills.csv` (WildDinoKill Plugin) fuer zusaetzliche Dino-Kill-Daten

Und bietet:

- Discord-Embeds mit Event-Feed
- Anti-Flood/Burst-Buendelung
- Persistente SQLite-Stats
- Slash-Commands:
  - `/leaderboard`
  - `/lastkill <playername>`

## 2. Voraussetzungen

- Python 3.11+ empfohlen
- Discord Bot Token
- Discord Channel-ID
- Schreibrechte im Projektverzeichnis (fuer DB/Logs)

## 3. Installation

### 3.1 Windows

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

### 3.2 Ubuntu / Linux

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip sqlite3

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## 4. Einrichtung / Konfiguration

### 4.1 Pflichtwerte in `.env`

- `DISCORD_TOKEN`
- `DISCORD_CHANNEL_ID`
- `ARK_LOG_PATH`

### 4.2 Wichtige optionale Werte

- `DISCORD_GUILD_ID`
  - wenn gesetzt: Slash-Commands werden guild-spezifisch synchronisiert (sichtbar meist sofort)
  - wenn leer: globale Synchronisierung (kann verzoegert sichtbar sein)
- `ARK_DB_PATH` (Default `ark_stats.db`)
- `ARK_RULES_PATH` (Default `rules.json`)
- `POLL_INTERVAL_SECONDS` (Default `1.5`)
- `ARK_LOG_HARD_REOPEN_INTERVAL_SECONDS` (Default `900`)

### 4.3 WildDinoKill CSV (optional)

- `ARK_WILD_KILLS_FEATURE_ENABLED` (`true`/`false`, Default `false`)
- `ARK_WILD_KILLS_CSV_PATH` (Pfad zu `wild_kills.csv`)

Hinweise:

- CSV darf in einem anderen Verzeichnis liegen als `ShooterGame.log`.
- CSV-Ingest ist restart-sicher (Leseposition in SQLite-Tabelle `ingestion_offsets`).

### 4.4 Discord-Posting und Telemetrie

- `ARK_DISCORD_POSTING_ENABLED` (`true`/`false`, Default `true`)
  - `false` = keine Discord-Posts, Verarbeitung/DB laufen weiter
  - in Kombination mit `ARK_LOG_DISCORD_MESSAGES=true` werden Event-Nachrichten als Dry-Run im Bot-Log protokolliert (`DISCORD_MESSAGE_DRYRUN`)
- `ARK_DB_DISCORD_LOG_ENABLED` (`true`/`false`, Default `false`)
  - kompakte DB-Telemetrie in Discord
- `ARK_DB_DISCORD_LOG_INTERVAL_SECONDS` (Default `300`)

### 4.5 Logging

- `ARK_LOG_FILE` (Default `ark_discord_bot.log`)
- `ARK_LOG_LEVEL` (Konsole)
- `ARK_LOG_FILE_LEVEL` (Datei)
- `ARK_LOG_DISCORD_MESSAGES` (`true`/`false`)
- `ARK_DISCORD_MESSAGE_DEBUG` (`true`/`false`)

## 5. Erststart

Bot starten:

```bash
source .venv/bin/activate
python bot.py
```

Beim ersten Start:

- SQLite-DB wird erzeugt/initialisiert
- Tabellen werden per `CREATE TABLE IF NOT EXISTS` angelegt
- Slash-Commands werden synchronisiert

## 6. Tests

## 6.1 Test-Abhaengigkeiten installieren

```bash
pip install -r requirements-dev.txt
```

## 6.2 Tests ausfuehren

Alle Tests:

```bash
python3 -m pytest -q
```

Nur Bot-Core:

```bash
python3 -m pytest -q tests/test_bot_core.py
```

## 6.3 Welche Tests es gibt und was sie pruefen

Datei: `tests/test_bot_core.py`

1. `test_rule_engine_matches_player_death_with_tribe_name`
- prueft, dass Death-Logzeilen mit Tribe-Tag in Klammern korrekt als `player_death_by` erkannt werden
- verhindert Fehlklassifikation solcher Zeilen als Dino-Kill

2. `test_wild_kill_csv_tail_parses_rows_and_skips_repeated_header`
- prueft CSV-Ingest inkl. wiederholter Headerzeilen
- prueft korrekte Extraktion von `killer_name`, Dino-Typ und Zeitstempel

3. `test_record_player_death_normalizes_player_name`
- prueft, dass Playernamen mit Suffix (`- Lvl ... (...) was`) normalisiert gespeichert werden
- erwartet `Ollinator` statt levelabhaengiger Varianten

4. `test_normalization_migrates_and_merges_existing_level_based_duplicates`
- prueft die Migrationslogik fuer bestehende DB-Dubletten
- stellt sicher, dass mehrere levelbasierte Eintraege zu einem Player zusammengefuehrt werden
- prueft Summenbildung bei Stats

5. `test_record_and_fetch_last_dino_kill_uses_normalized_player_name`
- prueft Persistierung von Dino-Kills in Event- und Aggregat-Tabellen
- prueft Lookup fuer `/lastkill` mit normalisiertem Playernamen

## 7. Betrieb

## 7.1 Slash-Commands

- `/leaderboard dino_kills`
- `/leaderboard player_kills`
- `/leaderboard dino_tames`
- `/leaderboard all`
- `/lastkill <playername>`

## 7.2 Regelanpassungen (`rules.json`)

Wenn Zeilen nicht erkannt werden:

1. echte Rohzeilen aus deinem Log nehmen
2. `pattern` in `rules.json` anpassen
3. Bot neu starten

## 7.3 Empfohlene Discord-Rechte

- View Channels
- Send Messages
- Embed Links
- Read Message History
- Use Slash Commands

## 8. Debugging und Monitoring

## 8.1 Bot-Log live ansehen

```bash
tail -f ark_discord_bot.log
```

## 8.2 SQLite Schnellchecks

Letzter Event-Eintrag:

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

Letztes CSV-Ingest-Update:

```bash
sqlite3 ark_stats.db "SELECT MAX(updated_at) AS last_ingest_update_utc FROM ingestion_offsets;"
```

Live-Check auf Aktivitaet:

```bash
watch -n 5 "sqlite3 ark_stats.db \"SELECT MAX(updated_at) FROM player_stats;\""
```

## 8.3 Typische Probleme

`/lastkill` nicht sichtbar:

- `DISCORD_GUILD_ID` setzen
- Bot neu starten
- in Discord Slash-Cache aktualisieren (neu oeffnen/reload)

`no such table: player_death_events`:

- Bot mit aktuellem Code einmal starten, damit Migration/Schema angelegt wird
- `ARK_DB_PATH` pruefen (richtige DB-Datei?)

Spieler doppelt mit `- Lvl ...`:

- aktueller Code normalisiert Namen
- beim Start werden alte Dubletten zusammengefuehrt

## 9. Datenbankschema (SQLite)

Die DB speichert:

- Stammdaten (`players`, `tribes`)
- Beziehungen (`player_tribe_membership`)
- Aggregierte Stats (`player_stats`, `player_dino_kills_by_type`)
- Event-Historie (`dino_tame_events`, `player_kill_events`, `player_death_events`, `dino_kill_events`)
- Ingestion-Fortschritt (`ingestion_offsets`)

Wichtige Tabellen:

- `players`
  - `id`, `player_name` (UNIQUE, NOCASE), `first_seen_at`, `last_seen_at`
  - Name wird normalisiert (z. B. `- Lvl ... (...) was` wird abgeschnitten)
  - Dino-Killer mit Muster `Name - Lvl X (Typ) (Tribe) was` werden als `Name (Typ)` gespeichert
- `player_stats`
  - `dino_kills_total`, `player_kills_total`, `dino_tames_total`
- `player_dino_kills_by_type`
  - Kills pro Spieler und Dino-Typ
- `dino_kill_events`
  - Einzelne Dino-Kills inkl. `source` (z. B. `shootergame_log`, `wild_kills_csv`)
- `player_death_events`
  - Einzelne Player-Deaths, auch ohne validen Player-Killer
- `ingestion_offsets`
  - Byte-Offsets pro Quelle fuer restart-sicheren CSV-Ingest

## 10. Projektdateien

- `bot.py` - Hauptlogik (Tailer, Parser, Discord, SQLite)
- `rules.json` - Regex-Regeln fuer Events
- `.env.example` - Konfigurationsvorlage
- `requirements.txt` - Runtime-Abhaengigkeiten
- `requirements-dev.txt` - Test-Abhaengigkeiten
- `tests/test_bot_core.py` - Kern-Tests
