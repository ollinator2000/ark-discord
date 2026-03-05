# ARK Ascended Discord Gamelog Bot (Windows 10)

Bot für **ARK: Survival Ascended PvPvE**, der dein Gamelog live parst und die wichtigsten Events als optisch aufbereitete Discord-Embeds in einen Channel schreibt.

## Features

- Live-Parsing des ARK-Gamelogs (Tail-Modus)
- PvP- und PvE-Events per Regex-Regeln
- Eye-Candy Ausgabe mit Emoji, Farben, Feldern, Timestamp
- Regeln anpassbar über `rules.json` (ohne Code-Änderung)
- Duplicate-Schutz für wiederholte Zeilen
- Anti-Flood: Burst-Events werden in Sammel-Embeds gebündelt

## Voraussetzungen

- Windows 10
- Python 3.11+ empfohlen
- Discord Bot Token + Zugriff auf Ziel-Channel

## Installation

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
- `POLL_INTERVAL_SECONDS`: z. B. `1.5`
- `BURST_TOP_ITEMS`: Anzahl Top-Items im Sammel-Embed (z. B. `5`)
- `BURST_MAX_BUFFER_SIZE`: Sofort-Flush bei sehr großem Burst (z. B. `250`)

## Start

```powershell
python bot.py
```

## Bot-Rechte in Discord

Empfohlen:

- View Channels
- Send Messages
- Embed Links
- Read Message History

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
