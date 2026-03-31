import asyncio
import csv
import json
import logging
import os
import re
import sqlite3
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import discord
from discord import app_commands
from dotenv import load_dotenv


logger = logging.getLogger("ark-log-bot")
DISCORD_MESSAGE_LOG_MARKER = "DISCORD_MESSAGE"
PLAYER_LEVEL_SUFFIX_RE = re.compile(r"\s*-\s*Lvl\s+\d+\s*\([^)]*\)\s*(?:was)?\s*$", re.IGNORECASE)
LEVELED_ENTITY_RE = re.compile(
    r"^(?P<name>.+?)\s*-\s*Lvl\s+\d+\s+\((?P<tag1>[^)]*)\)(?:\s+\((?P<tag2>[^)]*)\))?\s*(?:was)?\s*$",
    re.IGNORECASE,
)


def _parse_log_level(value: str, default_level: int) -> int:
    level = value.strip().upper()
    if not level:
        return default_level
    return getattr(logging, level, default_level)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on", "y"}


def configure_logging() -> None:
    file_path = os.getenv("ARK_LOG_FILE", "ark_discord_bot.log").strip() or "ark_discord_bot.log"
    file_level = _parse_log_level(os.getenv("ARK_LOG_FILE_LEVEL", os.getenv("ARK_LOG_LEVEL", "INFO")), logging.INFO)
    stream_level = _parse_log_level(os.getenv("ARK_LOG_LEVEL", "INFO"), logging.INFO)

    handlers: list[logging.Handler] = []

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(stream_level)
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"),
    )
    handlers.append(stream_handler)

    if file_path:
        log_dir = Path(file_path).parent
        if str(log_dir) not in {"", "."}:
            log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(file_path, encoding="utf-8")
        file_handler.setLevel(file_level)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(pathname)s:%(lineno)d | %(message)s"
            ),
        )
        handlers.append(file_handler)

    logger.setLevel(min(stream_level, file_level))
    logger.handlers = []
    for handler in handlers:
        logger.addHandler(handler)
    logger.propagate = False
    logger.info("Logging initialisiert. Datei=%s, level=stream(%s), file(%s)", file_path, stream_level, file_level)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ParsedEvent:
    rule_name: str
    key: str
    event_class: str
    aggregate_key: str
    aggregation_window_seconds: float
    title: str
    description: str
    color: int
    emoji: str
    fields: list[tuple[str, str]]
    cooldown_seconds: float
    context: dict[str, str]


class RuleEngine:
    def __init__(self, rules_path: Path):
        self.rules_path = rules_path
        self.rules: list[dict[str, Any]] = []
        self._compiled_rules: list[dict[str, Any]] = []
        self.load_rules()

    def load_rules(self) -> None:
        logger.info("Lade Regeln aus %s", self.rules_path)
        with self.rules_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        self.rules = data.get("rules", [])
        compiled: list[dict[str, Any]] = []

        for rule in self.rules:
            pattern = rule.get("pattern")
            if not pattern:
                continue
            try:
                regex = re.compile(pattern)
            except re.error as exc:
                logger.error("Regex-Fehler in Regel '%s': %s", rule.get("name", "unknown"), exc)
                continue

            compiled.append(
                {
                    "name": rule.get("name", "unnamed"),
                    "regex": regex,
                    "title": rule.get("title", "ARK Event"),
                    "description": rule.get("description", "{line}"),
                    "color": int(rule.get("color", 0x95A5A6)),
                    "emoji": rule.get("emoji", "📌"),
                    "fields": rule.get("fields", []),
                    "cooldown_seconds": float(rule.get("cooldown_seconds", 0)),
                    "event_class": str(rule.get("event_class", "normal")).lower(),
                    "aggregate_key": rule.get("aggregate_key", "{description}"),
                    "aggregation_window_seconds": float(rule.get("aggregation_window_seconds", 0)),
                }
            )

        self._compiled_rules = compiled
        logger.info("%d Regeln geladen", len(self._compiled_rules))

    def parse_line(self, line: str) -> ParsedEvent | None:
        for rule in self._compiled_rules:
            match = rule["regex"].search(line)
            if not match:
                continue

            groups = {k: v.strip() for k, v in match.groupdict().items() if v is not None}
            context = {"line": line.strip(), **groups}

            description = self._safe_format(rule["description"], context)
            title = self._safe_format(rule["title"], context)
            aggregate_key = self._safe_format(rule["aggregate_key"], {**context, "description": description})

            fields: list[tuple[str, str]] = []
            for field in rule["fields"]:
                name_tpl = field.get("name", "Info")
                value_tpl = field.get("value", "-")
                name = self._safe_format(name_tpl, context)
                value = self._safe_format(value_tpl, context)
                if value and value != "-":
                    fields.append((name, value))

            event_key = f"{rule['name']}|{line.strip()}"
            return ParsedEvent(
                rule_name=rule["name"],
                key=event_key,
                event_class=rule["event_class"],
                aggregate_key=aggregate_key,
                aggregation_window_seconds=rule["aggregation_window_seconds"],
                title=title,
                description=description,
                color=rule["color"],
                emoji=rule["emoji"],
                fields=fields,
                cooldown_seconds=rule["cooldown_seconds"],
                context=context,
            )

        return None

    @staticmethod
    def _safe_format(template: str, context: dict[str, str]) -> str:
        class SafeDict(dict):
            def __missing__(self, key: str) -> str:
                return f"{{{key}}}"

        return template.format_map(SafeDict(context))


class LogTail:
    def __init__(self, path: Path, hard_reopen_interval_seconds: float = 900.0):
        self.configured_path = path
        self._path_prefixes = self._build_prefixes(self.configured_path.stem)
        self._active_file_key: tuple[int, int] | None = None
        self._active_path: Path | None = None
        self._hard_reopen_interval_seconds = max(0.0, float(hard_reopen_interval_seconds))
        self._last_hard_reopen_ts = datetime.now(timezone.utc).timestamp()
        self._last_active_log_report = 0.0
        self.position = 0
        self._silent_poll_count = 0
        self._max_silent_polls_before_switch = 12
        self.path = self._resolve_current_file()
        if self.path is not None:
            try:
                st = self.path.stat()
                self._active_path = self.path
                self._active_file_key = (st.st_dev, st.st_ino)
            except OSError:
                self.path = None

    def _can_hard_reopen(self) -> bool:
        if self._hard_reopen_interval_seconds <= 0:
            return False
        return (datetime.now(timezone.utc).timestamp() - self._last_hard_reopen_ts) >= self._hard_reopen_interval_seconds

    def _maybe_hard_reopen(self) -> None:
        if not self._can_hard_reopen():
            return

        logger.info(
            "Erzwungener Logfile-Rescan (Intervall %.0fs)",
            self._hard_reopen_interval_seconds,
        )
        self._last_hard_reopen_ts = datetime.now(timezone.utc).timestamp()

        fresh = self._resolve_current_file(allow_stale_active=False)
        if fresh is None:
            logger.debug("Kein Logfile bei erzwungenem Rescan gefunden.")
            self._active_path = None
            self._active_file_key = None
            self.position = 0
            return

        try:
            st = fresh.stat()
            fresh_key = (st.st_dev, st.st_ino)
        except OSError:
            return

        if self._active_path is not None and fresh == self._active_path and fresh_key == self._active_file_key:
            try:
                current_size = fresh.stat().st_size
            except OSError:
                return
            if current_size < self.position:
                logger.warning(
                    "Aktives Logfile nach Reopen verkuerzt, Position wird nachgezogen: %s",
                    fresh,
                )
                self.position = current_size
            else:
                logger.debug("Logfile unveraendert bei Reopen, Position bleibt auf %s", self.position)
            self._silent_poll_count = 0
            return

        old = self._active_path
        self._active_path = fresh
        self._active_file_key = fresh_key
        self.position = 0
        self._silent_poll_count = 0
        logger.warning(
            "Logfile-Wechsel durch erzwungenen Reopen: %s -> %s",
            old,
            fresh,
        )

    def _log_active_file(self) -> None:
        if self._active_path is None:
            return

        now = datetime.now(timezone.utc).timestamp()
        if (now - self._last_active_log_report) < 60:
            return

        logger.info("Aktives Logfile: %s", self._active_path)
        self._last_active_log_report = now

    @staticmethod
    def _build_prefixes(stem: str) -> list[str]:
        prefixes = []
        if not stem:
            return prefixes

        prefixes.append(stem)
        for sep in ("-", "_"):
            head = stem.split(sep, 1)[0]
            if head and head not in prefixes:
                prefixes.append(head)
        return prefixes

    @staticmethod
    def _file_key(path: Path) -> tuple[int, int] | None:
        try:
            st = path.stat()
        except OSError:
            return None
        return st.st_dev, st.st_ino

    def _configured_path_rotated(self) -> bool:
        if self._active_path is None or self._active_file_key is None:
            return False

        configured_key = self._file_key(self.configured_path)
        if configured_key is None:
            return False
        if self._active_path != self.configured_path:
            return False
        return configured_key != self._active_file_key

    def _candidate_files(self) -> list[Path]:
        candidates: list[Path] = []
        if self.configured_path.exists():
            candidates.append(self.configured_path)

        parent = self.configured_path.parent
        if not parent.exists() or not parent.is_dir():
            return candidates

        base_name = self.configured_path.name
        if base_name:
            all_logs = parent.glob(f"{base_name}*")
        else:
            all_logs = parent.glob("*.log")

        if not base_name and self._path_prefixes:
            preferred: list[Path] = []
            for prefix in self._path_prefixes:
                preferred.extend(parent.glob(f"{prefix}*"))
            all_logs = preferred

        deduped = {str(path.resolve()): path for path in all_logs if path.is_file()}
        files = sorted(deduped.values(), key=lambda p: (p.stat().st_mtime_ns, p.name))
        logger.debug("Log-Kandidaten (%s): %s", len(files), [p.name for p in files])
        return files

    def _resolve_current_file(self, allow_stale_active: bool = True) -> Path | None:
        # Primary source is always the configured live file path (ShooterGame.log).
        # Only fall back to candidate discovery when that file is temporarily missing.
        if self.configured_path.exists():
            active_path = getattr(self, "_active_path", None)
            if active_path is not None and active_path != self.configured_path:
                logger.warning(
                    "Wechsel zur konfigurierten Live-Logdatei: %s (statt Archivdatei %s)",
                    self.configured_path,
                    active_path,
                )
            return self.configured_path

        candidates = self._candidate_files()
        if not candidates:
            return None

        active_path = getattr(self, "_active_path", None)
        active_key = getattr(self, "_active_file_key", None)

        if allow_stale_active and self._configured_path_rotated():
            logger.warning(
                "Ark Logfile wurde gedreht (Inode-Wechsel): %s",
                self.configured_path,
            )
            return self.configured_path

        if (
            allow_stale_active
            and active_path is not None
            and active_path in candidates
            and self._file_key(active_path) == active_key
            and self._is_file_active(active_path)
        ):
            return active_path

        selected = candidates[-1]
        if selected != self.configured_path:
            logger.debug(
                "Konfiguriertes Logfile fehlt temporär, nutze Kandidat: %s",
                selected,
            )
        return selected

    @staticmethod
    def _is_file_active(path: Path) -> bool:
        try:
            age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
        except OSError:
            return False

        return age_seconds < 45.0

    def _read_from_path(self, path: Path) -> tuple[list[str], int]:
        lines: list[str] = []
        start_position = self.position
        with path.open("r", encoding="utf-8", errors="replace") as f:
            f.seek(self.position)
            chunk = f.read()
            new_position = f.tell()

        if not chunk:
            logger.debug("Keine neuen Zeilen in %s (Position %s)", path, start_position)
            return lines, new_position

        logger.debug("Gelesen %s Bytes aus %s (von Pos %s auf %s)", len(chunk), path, start_position, new_position)

        for line in chunk.splitlines():
            cleaned = line.strip()
            if cleaned:
                lines.append(cleaned)

        return lines, new_position

    def read_new_lines(self) -> list[str]:
        self._maybe_hard_reopen()

        current = self._resolve_current_file()
        if current is None:
            if self._active_path is not None:
                logger.warning("Kein ARK Logfile gefunden fuer: %s", self.configured_path)
                self._active_path = None
                self._active_file_key = None
            return []

        key: tuple[int, int] | None = None
        try:
            st = current.stat()
            key = (st.st_dev, st.st_ino)
        except OSError:
            return []

        if self._active_path is None or key != self._active_file_key:
            self._active_path = current
            self._active_file_key = key
            self._silent_poll_count = 0
            self.position = 0
            logger.info("Log-Source gewechselt auf %s", current)
        else:
            self._log_active_file()

        if self._active_path is None:
            return []

        current_size = current.stat().st_size
        if current_size < self.position:
            self.position = 0

        lines, new_position = self._read_from_path(current)
        self.position = new_position

        if not lines:
            self._silent_poll_count += 1
            if self._silent_poll_count < self._max_silent_polls_before_switch:
                return []

            fresh = self._resolve_current_file(allow_stale_active=False)
            if fresh is None or fresh == current:
                return []

            try:
                fresh_stat = fresh.stat()
            except OSError:
                return []
            fresh_key = (fresh_stat.st_dev, fresh_stat.st_ino)
            if fresh_key == key:
                return []

            self._active_path = fresh
            self._active_file_key = fresh_key
            self._silent_poll_count = 0
            self.position = 0
            logger.warning(
                "Log-Quelle wurde still nach Rotation vermutet. Wechsel auf neue Datei: %s (alt: %s)",
                fresh,
                current,
            )
            lines, self.position = self._read_from_path(fresh)
            return lines

        self._silent_poll_count = 0
        return lines


class WildKillCsvTail:
    def __init__(self, path: Path):
        self.path = path
        self.position = 0
        self._last_missing_log_ts = 0.0

    def set_position(self, position: int) -> None:
        self.position = max(0, int(position))

    @staticmethod
    def _extract_dino_name(blueprint: str) -> str:
        raw = blueprint.strip()
        if not raw:
            return ""

        token = raw.rstrip("'").split("/")[-1]
        token = token.split(".", 1)[0]
        for suffix in ("_Character_BP_C", "_Character_BP", "_BP_C", "_BP"):
            if token.endswith(suffix):
                token = token[: -len(suffix)]
                break
        return token.strip().replace("_", " ")

    def read_new_kills(self) -> tuple[list[tuple[str, str, str | None]], int]:
        now_ts = datetime.now(timezone.utc).timestamp()
        if not self.path.exists():
            if (now_ts - self._last_missing_log_ts) >= 60.0:
                logger.warning("WildKill CSV nicht gefunden: %s", self.path)
                self._last_missing_log_ts = now_ts
            return [], self.position

        try:
            file_size = self.path.stat().st_size
        except OSError:
            return [], self.position

        if file_size < self.position:
            logger.warning("WildKill CSV wurde gekuerzt/rotiert, starte neu ab Byte 0: %s", self.path)
            self.position = 0

        start_position = self.position
        with self.path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            f.seek(self.position)
            chunk = f.read()
            new_position = f.tell()

        if not chunk:
            return [], new_position

        kills: list[tuple[str, str, str | None]] = []
        for raw_line in chunk.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            try:
                row = next(csv.reader([line]))
            except Exception:
                logger.warning("WildKill CSV Zeile konnte nicht geparst werden: %s", line)
                continue

            if not row:
                continue
            if row[0].strip().lower() == "timestamp_utc":
                # Header-Zeilen koennen im laufenden Betrieb erneut auftauchen.
                continue
            if len(row) < 7:
                logger.debug("WildKill CSV Zeile mit zu wenigen Spalten ignoriert: %s", row)
                continue

            killer_name = row[6].strip()
            event_time_text = row[0].strip() if row else None
            dino_name = self._extract_dino_name(row[1])
            if not killer_name or not dino_name:
                continue
            kills.append((killer_name, dino_name, event_time_text))

        logger.debug(
            "WildKill CSV gelesen: %s Bytes von %s bis %s, kills=%s",
            len(chunk),
            start_position,
            new_position,
            len(kills),
        )
        self.position = new_position
        return kills, new_position


class StatsStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        logger.info("Initialisiere SQLite DB: %s", self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._db_metrics: dict[str, int] = {"reads": 0, "writes": 0, "commits": 0}
        self._batch_mode = False
        self._batch_pending_commit = False
        self._init_schema()
        self._normalize_existing_player_rows()
        logger.info("SQLite DB bereit: %s", self.db_path)

    @staticmethod
    def normalize_player_name(raw_name: str) -> str:
        normalized = (raw_name or "").strip()
        if not normalized:
            return ""
        normalized = PLAYER_LEVEL_SUFFIX_RE.sub("", normalized).strip()
        return normalized

    @staticmethod
    def normalize_identity_name(raw_name: str) -> str:
        raw = (raw_name or "").strip()
        if not raw:
            return ""

        match = LEVELED_ENTITY_RE.match(raw)
        if match:
            name = (match.group("name") or "").strip()
            tag1 = (match.group("tag1") or "").strip()
            tag2 = (match.group("tag2") or "").strip()
            if name and tag1 and tag2:
                # Dino-Killer-Format: Name - Lvl X (Species) (Tribe) was
                return f"{name} ({tag1})"
            if name:
                # Spielerformat (oder nicht eindeutig): Name - Lvl X (Tribe) was
                return name

        return StatsStore.normalize_player_name(raw)

    def _init_schema(self) -> None:
        logger.debug("Erstelle/prüfe Datenbankschema: %s", self.db_path)
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tribes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tribe_name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS player_tribe_membership (
                player_id INTEGER NOT NULL,
                tribe_id INTEGER NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (player_id, tribe_id),
                FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE,
                FOREIGN KEY (tribe_id) REFERENCES tribes(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS player_stats (
                player_id INTEGER PRIMARY KEY,
                dino_kills_total INTEGER NOT NULL DEFAULT 0,
                player_kills_total INTEGER NOT NULL DEFAULT 0,
                dino_tames_total INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS player_dino_kills_by_type (
                player_id INTEGER NOT NULL,
                dino_type TEXT NOT NULL,
                kills_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (player_id, dino_type),
                FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS dino_tame_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                tribe_id INTEGER,
                dino_type TEXT NOT NULL,
                dino_level INTEGER,
                event_time_text TEXT,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (player_id) REFERENCES players(id) ON DELETE CASCADE,
                FOREIGN KEY (tribe_id) REFERENCES tribes(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS player_kill_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                killer_player_id INTEGER NOT NULL,
                victim_name TEXT,
                victim_player_id INTEGER,
                event_time_text TEXT,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (killer_player_id) REFERENCES players(id) ON DELETE CASCADE,
                FOREIGN KEY (victim_player_id) REFERENCES players(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS player_death_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                victim_player_id INTEGER,
                victim_name TEXT,
                killer_text TEXT,
                event_time_text TEXT,
                source_rule TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (victim_player_id) REFERENCES players(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS dino_kill_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                killer_player_id INTEGER NOT NULL,
                dino_type TEXT NOT NULL,
                event_time_text TEXT,
                source TEXT,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (killer_player_id) REFERENCES players(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS ingestion_offsets (
                source_key TEXT PRIMARY KEY,
                position INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_players_name ON players(player_name);
            CREATE INDEX IF NOT EXISTS idx_tribes_name ON tribes(tribe_name);
            CREATE INDEX IF NOT EXISTS idx_dino_tame_player ON dino_tame_events(player_id);
            CREATE INDEX IF NOT EXISTS idx_player_kill_killer ON player_kill_events(killer_player_id);
            CREATE INDEX IF NOT EXISTS idx_player_death_victim ON player_death_events(victim_player_id);
            CREATE INDEX IF NOT EXISTS idx_dino_kill_player ON dino_kill_events(killer_player_id);
            """
        )
        self.conn.commit()
        logger.debug("Schema check abgeschlossen: %s", self.db_path)

    def _log_sql(self, statement: str, params: tuple[Any, ...] | list[Any] | None = None) -> None:
        if params is None:
            logger.debug("SQL: %s", statement)
        else:
            logger.debug("SQL: %s | params=%s", statement, params)

    def _metric_inc(self, key: str, amount: int = 1) -> None:
        self._db_metrics[key] = self._db_metrics.get(key, 0) + amount

    def _commit_locked(self) -> None:
        if self._batch_mode:
            self._batch_pending_commit = True
            return
        self.conn.commit()
        self._metric_inc("commits")

    @asynccontextmanager
    async def write_batch(self):
        async with self._lock:
            self._batch_mode = True
            self._batch_pending_commit = False
        try:
            yield
        finally:
            async with self._lock:
                self._batch_mode = False
                if self._batch_pending_commit:
                    self.conn.commit()
                    self._metric_inc("commits")
                self._batch_pending_commit = False

    def _normalize_existing_player_rows(self) -> None:
        rows = self.conn.execute("SELECT id, player_name FROM players ORDER BY id").fetchall()
        if not rows:
            return

        merges = 0
        renames = 0
        for row in rows:
            source_id = int(row["id"])
            source_name = str(row["player_name"])
            normalized_name = self.normalize_identity_name(source_name)
            if not normalized_name or normalized_name == source_name:
                continue

            target_id = self._get_player_id_locked(normalized_name)
            if target_id is None:
                try:
                    self.conn.execute(
                        "UPDATE players SET player_name = ? WHERE id = ?",
                        (normalized_name, source_id),
                    )
                    renames += 1
                except sqlite3.IntegrityError:
                    target_id = self._get_player_id_locked(normalized_name)

            if target_id is not None and target_id != source_id:
                self._merge_player_ids_locked(source_id=source_id, target_id=target_id)
                merges += 1

        if renames or merges:
            self.conn.commit()
            logger.info(
                "Player-Normalisierung durchgefuehrt: umbenannt=%s zusammengefuehrt=%s",
                renames,
                merges,
            )

    def _merge_player_ids_locked(self, source_id: int, target_id: int) -> None:
        if source_id == target_id:
            return

        source_player = self.conn.execute(
            "SELECT first_seen_at, last_seen_at FROM players WHERE id = ?",
            (source_id,),
        ).fetchone()
        target_player = self.conn.execute(
            "SELECT first_seen_at, last_seen_at FROM players WHERE id = ?",
            (target_id,),
        ).fetchone()
        if source_player is None or target_player is None:
            return

        min_first_seen = min(str(source_player["first_seen_at"]), str(target_player["first_seen_at"]))
        max_last_seen = max(str(source_player["last_seen_at"]), str(target_player["last_seen_at"]))
        self.conn.execute(
            "UPDATE players SET first_seen_at = ?, last_seen_at = ? WHERE id = ?",
            (min_first_seen, max_last_seen, target_id),
        )

        source_stats = self.conn.execute(
            "SELECT dino_kills_total, player_kills_total, dino_tames_total FROM player_stats WHERE player_id = ?",
            (source_id,),
        ).fetchone()
        if source_stats is not None:
            now = utc_now_iso()
            self.conn.execute(
                """
                INSERT INTO player_stats (player_id, dino_kills_total, player_kills_total, dino_tames_total, updated_at)
                VALUES (?, 0, 0, 0, ?)
                ON CONFLICT(player_id) DO NOTHING
                """,
                (target_id, now),
            )
            self.conn.execute(
                """
                UPDATE player_stats
                SET dino_kills_total = dino_kills_total + ?,
                    player_kills_total = player_kills_total + ?,
                    dino_tames_total = dino_tames_total + ?,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (
                    int(source_stats["dino_kills_total"]),
                    int(source_stats["player_kills_total"]),
                    int(source_stats["dino_tames_total"]),
                    now,
                    target_id,
                ),
            )
            self.conn.execute("DELETE FROM player_stats WHERE player_id = ?", (source_id,))

        source_dino_rows = self.conn.execute(
            "SELECT dino_type, kills_count, updated_at FROM player_dino_kills_by_type WHERE player_id = ?",
            (source_id,),
        ).fetchall()
        for row in source_dino_rows:
            self.conn.execute(
                """
                INSERT INTO player_dino_kills_by_type (player_id, dino_type, kills_count, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(player_id, dino_type) DO UPDATE
                SET kills_count = player_dino_kills_by_type.kills_count + excluded.kills_count,
                    updated_at = excluded.updated_at
                """,
                (target_id, str(row["dino_type"]), int(row["kills_count"]), str(row["updated_at"])),
            )
        self.conn.execute("DELETE FROM player_dino_kills_by_type WHERE player_id = ?", (source_id,))

        source_memberships = self.conn.execute(
            "SELECT tribe_id, last_seen_at FROM player_tribe_membership WHERE player_id = ?",
            (source_id,),
        ).fetchall()
        for row in source_memberships:
            self.conn.execute(
                """
                INSERT INTO player_tribe_membership (player_id, tribe_id, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player_id, tribe_id) DO UPDATE
                SET last_seen_at = excluded.last_seen_at
                """,
                (target_id, int(row["tribe_id"]), str(row["last_seen_at"])),
            )
        self.conn.execute("DELETE FROM player_tribe_membership WHERE player_id = ?", (source_id,))

        self.conn.execute("UPDATE dino_tame_events SET player_id = ? WHERE player_id = ?", (target_id, source_id))
        self.conn.execute("UPDATE player_kill_events SET killer_player_id = ? WHERE killer_player_id = ?", (target_id, source_id))
        self.conn.execute("UPDATE player_kill_events SET victim_player_id = ? WHERE victim_player_id = ?", (target_id, source_id))
        self.conn.execute("UPDATE dino_kill_events SET killer_player_id = ? WHERE killer_player_id = ?", (target_id, source_id))
        self.conn.execute("UPDATE player_death_events SET victim_player_id = ? WHERE victim_player_id = ?", (target_id, source_id))

        self.conn.execute("DELETE FROM players WHERE id = ?", (source_id,))

    async def record_player_seen(self, player_name: str) -> int | None:
        normalized = self.normalize_player_name(player_name)
        if not normalized:
            return None
        logger.debug("Persist Player Seen: %s", normalized)

        async with self._lock:
            now = utc_now_iso()
            self._log_sql(
                "INSERT INTO players ... ON CONFLICT",
                (normalized, now, now),
            )
            self.conn.execute(
                """
                INSERT INTO players (player_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player_name) DO UPDATE SET last_seen_at=excluded.last_seen_at
                """,
                (normalized, now, now),
            )
            self._commit_locked()
            self._metric_inc("writes")
            return self._get_player_id_locked(normalized)

    async def link_player_to_tribe(self, player_name: str, tribe_name: str) -> None:
        normalized_player = self.normalize_player_name(player_name)
        normalized_tribe = tribe_name.strip()
        if not normalized_player or not normalized_tribe:
            return
        logger.debug("Persist join tribe: player=%s tribe=%s", normalized_player, normalized_tribe)

        async with self._lock:
            now = utc_now_iso()
            self.conn.execute(
                """
                INSERT INTO players (player_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player_name) DO UPDATE SET last_seen_at=excluded.last_seen_at
                """,
                (normalized_player, now, now),
            )
            self.conn.execute(
                """
                INSERT INTO tribes (tribe_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(tribe_name) DO UPDATE SET last_seen_at=excluded.last_seen_at
                """,
                (normalized_tribe, now, now),
            )

            player_id = self._get_player_id_locked(normalized_player)
            tribe_id = self._get_tribe_id_locked(normalized_tribe)
            if player_id is None or tribe_id is None:
                self.conn.commit()
                return

            self.conn.execute(
                """
                INSERT INTO player_tribe_membership (player_id, tribe_id, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player_id, tribe_id) DO UPDATE SET last_seen_at=excluded.last_seen_at
                """,
                (player_id, tribe_id, now),
            )
            self._commit_locked()
            self._metric_inc("writes", 3)

    async def record_dino_kill(
        self,
        killer_name: str,
        dino_type: str,
        event_time_text: str | None = None,
        source: str | None = None,
    ) -> None:
        killer = self.normalize_identity_name(killer_name)
        dino = dino_type.strip()
        if not killer or not dino:
            return
        logger.debug("Persist dino kill: killer=%s dino=%s", killer, dino)

        async with self._lock:
            now = utc_now_iso()
            killer_id = self._ensure_player_locked(killer, now)
            self._ensure_player_stats_locked(killer_id, now)

            self.conn.execute(
                """
                UPDATE player_stats
                SET dino_kills_total = dino_kills_total + 1,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (now, killer_id),
            )
            self.conn.execute(
                """
                INSERT INTO player_dino_kills_by_type (player_id, dino_type, kills_count, updated_at)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(player_id, dino_type)
                DO UPDATE SET kills_count = kills_count + 1, updated_at=excluded.updated_at
                """,
                (killer_id, dino, now),
            )
            self.conn.execute(
                """
                INSERT INTO dino_kill_events (killer_player_id, dino_type, event_time_text, source, recorded_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (killer_id, dino, event_time_text, source, now),
            )
            self._commit_locked()
            self._metric_inc("writes", 3)

    async def record_dino_tame(self, player_name: str, dino_type: str, dino_level: int | None, tribe_name: str) -> None:
        player = self.normalize_player_name(player_name)
        dino = dino_type.strip()
        tribe = tribe_name.strip()
        if not player or not dino:
            return
        logger.debug("Persist dino tame: player=%s dino=%s level=%s tribe=%s", player, dino, dino_level, tribe)

        async with self._lock:
            now = utc_now_iso()
            player_id = self._ensure_player_locked(player, now)
            self._ensure_player_stats_locked(player_id, now)

            tribe_id: int | None = None
            if tribe:
                self.conn.execute(
                    """
                    INSERT INTO tribes (tribe_name, first_seen_at, last_seen_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(tribe_name) DO UPDATE SET last_seen_at=excluded.last_seen_at
                    """,
                    (tribe, now, now),
                )
                tribe_id = self._get_tribe_id_locked(tribe)
                if tribe_id is not None:
                    self.conn.execute(
                        """
                        INSERT INTO player_tribe_membership (player_id, tribe_id, last_seen_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(player_id, tribe_id) DO UPDATE SET last_seen_at=excluded.last_seen_at
                        """,
                        (player_id, tribe_id, now),
                    )

            self.conn.execute(
                """
                UPDATE player_stats
                SET dino_tames_total = dino_tames_total + 1,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (now, player_id),
            )
            self.conn.execute(
                """
                INSERT INTO dino_tame_events (player_id, tribe_id, dino_type, dino_level, event_time_text, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (player_id, tribe_id, dino, dino_level, None, now),
            )
            self._commit_locked()
            self._metric_inc("writes", 3)

    async def record_player_kill(self, killer_name: str, victim_name: str) -> None:
        killer = self.normalize_player_name(killer_name)
        victim = self.normalize_player_name(victim_name)
        if not killer:
            return
        logger.debug("Persist player kill: killer=%s victim=%s", killer, victim)

        async with self._lock:
            now = utc_now_iso()
            killer_id = self._ensure_player_locked(killer, now)
            self._ensure_player_stats_locked(killer_id, now)

            victim_player_id: int | None = None
            if victim:
                victim_player_id = self._ensure_player_locked(victim, now)

            self.conn.execute(
                """
                UPDATE player_stats
                SET player_kills_total = player_kills_total + 1,
                    updated_at = ?
                WHERE player_id = ?
                """,
                (now, killer_id),
            )
            self.conn.execute(
                """
                INSERT INTO player_kill_events (
                    killer_player_id, victim_name, victim_player_id, event_time_text, recorded_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (killer_id, victim if victim else None, victim_player_id, None, now),
            )
            self._commit_locked()
            self._metric_inc("writes", 2)

    async def record_player_death(
        self,
        victim_name: str,
        killer_text: str | None = None,
        event_time_text: str | None = None,
        source_rule: str = "unknown",
    ) -> None:
        victim = self.normalize_player_name(victim_name)
        if not victim:
            return
        killer = self.normalize_player_name(killer_text or "")
        logger.debug(
            "Persist player death: victim=%s killer=%s source=%s",
            victim,
            killer,
            source_rule,
        )

        async with self._lock:
            now = utc_now_iso()
            victim_id = self._ensure_player_locked(victim, now)
            self.conn.execute(
                """
                INSERT INTO player_death_events (
                    victim_player_id, victim_name, killer_text, event_time_text, source_rule, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    victim_id,
                    victim,
                    killer if killer else None,
                    event_time_text,
                    source_rule,
                    now,
                ),
            )
            self._commit_locked()
            self._metric_inc("writes")

    async def fetch_top_players(self, metric: str, limit: int = 5) -> list[tuple[str, int]]:
        metric_column_map = {
            "dino_kills": "dino_kills_total",
            "player_kills": "player_kills_total",
            "dino_tames": "dino_tames_total",
        }
        column = metric_column_map.get(metric)
        if column is None:
            return []
        logger.debug("Fetch leaderboard metric=%s limit=%s", metric, limit)

        async with self._lock:
            cur = self.conn.execute(
                f"""
                SELECT p.player_name, s.{column} AS score
                FROM player_stats s
                JOIN players p ON p.id = s.player_id
                WHERE s.{column} > 0
                ORDER BY s.{column} DESC, p.player_name ASC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
            self._metric_inc("reads")
            return [(str(row["player_name"]), int(row["score"])) for row in rows]

    async def get_ingestion_offset(self, source_key: str) -> int:
        async with self._lock:
            cur = self.conn.execute(
                "SELECT position FROM ingestion_offsets WHERE source_key = ?",
                (source_key,),
            )
            row = cur.fetchone()
            self._metric_inc("reads")
            if row is None:
                return 0
            return max(0, int(row["position"]))

    async def set_ingestion_offset(self, source_key: str, position: int) -> None:
        async with self._lock:
            now = utc_now_iso()
            self.conn.execute(
                """
                INSERT INTO ingestion_offsets (source_key, position, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE
                SET position = excluded.position,
                    updated_at = excluded.updated_at
                """,
                (source_key, max(0, int(position)), now),
            )
            self._commit_locked()
            self._metric_inc("writes")

    async def fetch_last_dino_kill_for_player(self, player_name: str) -> tuple[str, str, str, str] | None:
        normalized = self.normalize_player_name(player_name)
        if not normalized:
            return None

        async with self._lock:
            cur = self.conn.execute(
                """
                SELECT p.player_name, e.dino_type, COALESCE(e.event_time_text, e.recorded_at) AS event_time, COALESCE(e.source, 'unknown') AS source
                FROM dino_kill_events e
                JOIN players p ON p.id = e.killer_player_id
                WHERE p.player_name = ?
                ORDER BY e.id DESC
                LIMIT 1
                """,
                (normalized,),
            )
            row = cur.fetchone()
            self._metric_inc("reads")
            if row is None:
                return None
            return (
                str(row["player_name"]),
                str(row["dino_type"]),
                str(row["event_time"]),
                str(row["source"]),
            )

    async def pop_db_metrics(self) -> dict[str, int]:
        async with self._lock:
            snapshot = dict(self._db_metrics)
            self._db_metrics = {"reads": 0, "writes": 0, "commits": 0}
            return snapshot

    def _ensure_player_locked(self, player_name: str, now_iso: str) -> int:
        self.conn.execute(
            """
            INSERT INTO players (player_name, first_seen_at, last_seen_at)
            VALUES (?, ?, ?)
            ON CONFLICT(player_name) DO UPDATE SET last_seen_at=excluded.last_seen_at
            """,
            (player_name, now_iso, now_iso),
        )
        player_id = self._get_player_id_locked(player_name)
        if player_id is None:
            raise RuntimeError(f"Could not resolve player id for {player_name}")
        return player_id

    def _ensure_player_stats_locked(self, player_id: int, now_iso: str) -> None:
        self.conn.execute(
            """
            INSERT INTO player_stats (player_id, dino_kills_total, player_kills_total, dino_tames_total, updated_at)
            VALUES (?, 0, 0, 0, ?)
            ON CONFLICT(player_id) DO NOTHING
            """,
            (player_id, now_iso),
        )

    def _get_player_id_locked(self, player_name: str) -> int | None:
        cur = self.conn.execute("SELECT id FROM players WHERE player_name = ?", (player_name,))
        row = cur.fetchone()
        return int(row["id"]) if row else None

    def _get_tribe_id_locked(self, tribe_name: str) -> int | None:
        cur = self.conn.execute("SELECT id FROM tribes WHERE tribe_name = ?", (tribe_name,))
        row = cur.fetchone()
        return int(row["id"]) if row else None


class ArkLogBot(discord.Client):
    def __init__(
        self,
        channel_id: int,
        rule_engine: RuleEngine,
        tail: LogTail,
        wild_kill_csv_tail: WildKillCsvTail | None,
        wild_kill_feature_enabled: bool,
        stats_store: StatsStore,
        poll_interval: float,
        burst_top_items: int,
        burst_max_buffer_size: int,
        leaderboard_interval_seconds: int,
        discord_posting_enabled: bool,
        db_discord_log_enabled: bool,
        db_discord_log_interval_seconds: int,
    ):
        intents = discord.Intents.none()
        super().__init__(intents=intents)
        self.channel_id = channel_id
        self.rule_engine = rule_engine
        self.tail = tail
        self.wild_kill_csv_tail = wild_kill_csv_tail
        self.wild_kill_feature_enabled = wild_kill_feature_enabled and wild_kill_csv_tail is not None
        self._wild_kill_source_key = (
            f"wild_kills_csv:{self.wild_kill_csv_tail.path.resolve()}"
            if self.wild_kill_csv_tail is not None
            else ""
        )
        self._wild_kill_offset_loaded = False
        self.stats_store = stats_store
        self.poll_interval = poll_interval
        self.burst_top_items = burst_top_items
        self.burst_max_buffer_size = burst_max_buffer_size
        self.leaderboard_interval_seconds = leaderboard_interval_seconds
        self.discord_posting_enabled = discord_posting_enabled
        self.db_discord_log_enabled = db_discord_log_enabled
        self.db_discord_log_interval_seconds = max(10, int(db_discord_log_interval_seconds))

        self.recent_events = deque(maxlen=200)
        self.last_sent_by_rule: dict[str, float] = {}
        self.pending_burst_events: dict[str, list[ParsedEvent]] = {}
        self.pending_burst_started_ts: dict[str, float] = {}
        self.pending_burst_window_seconds: dict[str, float] = {}
        self.discord_message_debug = _env_bool("ARK_DISCORD_MESSAGE_DEBUG", True)
        self.log_discord_payloads = _env_bool("ARK_LOG_DISCORD_MESSAGES", True)

        self.bg_task: asyncio.Task | None = None
        self.leaderboard_task: asyncio.Task | None = None
        self.db_log_task: asyncio.Task | None = None

        self.tree = app_commands.CommandTree(self)
        self._commands_registered = False

    async def setup_hook(self) -> None:
        self._register_commands()
        guild_id_raw = os.getenv("DISCORD_GUILD_ID", "").strip()
        if guild_id_raw:
            guild = discord.Object(id=int(guild_id_raw))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
            logger.info("Slash Commands fuer Guild %s synchronisiert.", guild_id_raw)
            return

        await self.tree.sync()
        logger.info("Slash Commands global synchronisiert.")

    def _register_commands(self) -> None:
        if self._commands_registered:
            return

        @self.tree.command(name="leaderboard", description="Poste ein ARK Leaderboard")
        @app_commands.describe(board="Welches Leaderboard?")
        @app_commands.choices(
            board=[
                app_commands.Choice(name="Dino Kills", value="dino_kills"),
                app_commands.Choice(name="Player Kills", value="player_kills"),
                app_commands.Choice(name="Dino Tames", value="dino_tames"),
                app_commands.Choice(name="Alle", value="all"),
            ]
        )
        async def leaderboard(interaction: discord.Interaction, board: app_commands.Choice[str]) -> None:
            if not self.discord_posting_enabled:
                await interaction.response.send_message(
                    "Discord-Posting ist deaktiviert. Aktuelle Werte werden nur im Bot-Log protokolliert.",
                    ephemeral=True,
                )
                return

            if interaction.channel is None:
                await interaction.response.send_message("Kein gueltiger Channel.", ephemeral=True)
                return

            embeds = await self._build_leaderboard_embeds(board.value, requested_by="On-Demand")
            if not embeds:
                await interaction.response.send_message("Keine Daten fuer dieses Leaderboard vorhanden.")
                logger.warning("On-Demand Leaderboard ohne Daten: board=%s user=%s", board.value, interaction.user)
                return

            await interaction.response.send_message(embeds=embeds)
            logger.info("On-Demand Leaderboard gesendet: board=%s user=%s", board.value, interaction.user)

        @self.tree.command(name="lastkill", description="Zeigt den letzten Dino-Kill eines Spielers")
        @app_commands.describe(player="Exakter Spielername")
        async def lastkill(interaction: discord.Interaction, player: str) -> None:
            result = await self.stats_store.fetch_last_dino_kill_for_player(player)
            if result is None:
                await interaction.response.send_message(
                    f"Kein Dino-Kill fuer Spieler `{player}` gefunden.",
                    ephemeral=True,
                )
                return

            player_name, dino_name, event_time, source = result
            embed = discord.Embed(
                title="Letzter Dino-Kill",
                description=f"**{player_name}** hat zuletzt **{dino_name}** getoetet.",
                color=0x3498DB,
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(name="Zeit", value=event_time, inline=True)
            embed.add_field(name="Quelle", value=source, inline=True)
            embed.set_footer(text="ARK Leaderboard | Last Kill")

            await interaction.response.send_message(embed=embed)
            logger.info("Lastkill abgefragt: player=%s user=%s", player_name, interaction.user)

        @self.tree.command(name="discordposting", description="Discord-Posting zur Laufzeit steuern")
        @app_commands.describe(action="Aktion")
        @app_commands.choices(
            action=[
                app_commands.Choice(name="Status", value="status"),
                app_commands.Choice(name="Aktivieren", value="enable"),
                app_commands.Choice(name="Deaktivieren", value="disable"),
            ]
        )
        async def discordposting(interaction: discord.Interaction, action: app_commands.Choice[str]) -> None:
            if interaction.guild_id is not None:
                member = interaction.user if isinstance(interaction.user, discord.Member) else None
                if member is None or not member.guild_permissions.manage_guild:
                    await interaction.response.send_message(
                        "Du brauchst die Berechtigung `Manage Server`, um das Posting umzustellen.",
                        ephemeral=True,
                    )
                    return

            current_state = self.discord_posting_enabled
            requested = action.value

            if requested == "status":
                status_text = "aktiviert" if current_state else "deaktiviert"
                await interaction.response.send_message(
                    f"Discord-Posting ist aktuell **{status_text}**.",
                    ephemeral=True,
                )
                return

            if requested == "enable":
                self.discord_posting_enabled = True
            elif requested == "disable":
                self.discord_posting_enabled = False

            new_state = self.discord_posting_enabled
            if current_state == new_state:
                status_text = "aktiviert" if new_state else "deaktiviert"
                await interaction.response.send_message(
                    f"Discord-Posting ist bereits **{status_text}**.",
                    ephemeral=True,
                )
                return

            status_text = "aktiviert" if new_state else "deaktiviert"
            logger.info(
                "Runtime Toggle: discord_posting_enabled=%s user=%s guild=%s channel=%s",
                new_state,
                interaction.user,
                interaction.guild_id,
                interaction.channel_id,
            )
            await interaction.response.send_message(
                f"Discord-Posting wurde **{status_text}**.",
                ephemeral=True,
            )

        self._commands_registered = True

    async def on_ready(self) -> None:
        logger.info("Bot eingeloggt als %s (%s)", self.user, self.user.id if self.user else "?")
        if self.bg_task is None:
            self.bg_task = asyncio.create_task(self._watch_loop(), name="log-watch-loop")
        if self.leaderboard_task is None:
            self.leaderboard_task = asyncio.create_task(self._leaderboard_loop(), name="leaderboard-loop")
        if self.db_log_task is None:
            self.db_log_task = asyncio.create_task(self._db_log_loop(), name="db-log-loop")

    async def _resolve_channel(self) -> discord.TextChannel | None:
        channel = self.get_channel(self.channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(self.channel_id)
            except discord.NotFound:
                logger.error(
                    "Channel %s nicht gefunden. Bitte pruefe, ob es wirklich eine Textchannel-ID ist.",
                    self.channel_id,
                )
                return None
            except discord.Forbidden:
                logger.error(
                    "Kein Zugriff auf Channel %s. Bitte pruefe Bot-Rechte und Channel-Overrides.",
                    self.channel_id,
                )
                return None
            except discord.HTTPException as exc:
                logger.error("Discord API Fehler beim Laden von Channel %s: %s", self.channel_id, exc)
                return None

        if not isinstance(channel, discord.TextChannel):
            logger.error("Channel %s ist kein TextChannel.", self.channel_id)
            return None
        return channel

    async def _watch_loop(self) -> None:
        await self.wait_until_ready()
        channel: discord.TextChannel | None = None
        next_channel_resolve_ts = 0.0
        if not self.discord_posting_enabled:
            logger.info("Discord-Posting deaktiviert. Events werden nur im Logfile verarbeitet.")

        logger.info("Starte Log-Watcher fuer %s", self.tail.path)
        if self.wild_kill_feature_enabled and self.wild_kill_csv_tail is not None:
            logger.info("WildKill CSV Ingest aktiv: %s", self.wild_kill_csv_tail.path)
            if not self._wild_kill_offset_loaded:
                offset = await self.stats_store.get_ingestion_offset(self._wild_kill_source_key)
                self.wild_kill_csv_tail.set_position(offset)
                self._wild_kill_offset_loaded = True
                logger.info("WildKill CSV Start-Offset geladen: %s", offset)
        else:
            logger.info("WildKill CSV Ingest deaktiviert.")
        logger.info("Watchdog startet mit Poll-Intervall=%ss", self.poll_interval)

        while not self.is_closed():
            try:
                tick_start = datetime.now(timezone.utc).timestamp()
                if self.discord_posting_enabled and channel is None and tick_start >= next_channel_resolve_ts:
                    channel = await self._resolve_channel()
                    if channel is None:
                        next_channel_resolve_ts = tick_start + 30.0
                    else:
                        logger.info("Discord-Channel erfolgreich aufgeloest: %s", channel.id)
                        next_channel_resolve_ts = 0.0
                elif not self.discord_posting_enabled and channel is not None:
                    channel = None
                    logger.info("Discord-Posting deaktiviert. Wechsle in Dry-Run-Modus.")

                async with self.stats_store.write_batch():
                    new_lines = self.tail.read_new_lines()
                    logger.debug("Watch-Tick: %s neue Zeilen", len(new_lines))
                    for line in new_lines:
                        event = self.rule_engine.parse_line(line)
                        if event is None:
                            continue

                        await self._persist_event(event)

                        if event.event_class == "burst":
                            self._queue_burst_event(event)
                            continue

                        await self._send_immediate_event(channel, event)

                    if self.wild_kill_feature_enabled and self.wild_kill_csv_tail is not None:
                        previous_csv_position = self.wild_kill_csv_tail.position
                        csv_kills, new_csv_position = self.wild_kill_csv_tail.read_new_kills()
                        if csv_kills:
                            logger.info("WildKill CSV neue Kills: %s", len(csv_kills))
                        for killer_name, dino_name, event_time_text in csv_kills:
                            await self.stats_store.record_dino_kill(
                                killer_name=killer_name,
                                dino_type=dino_name,
                                event_time_text=event_time_text,
                                source="wild_kills_csv",
                            )
                        if new_csv_position != previous_csv_position:
                            await self.stats_store.set_ingestion_offset(self._wild_kill_source_key, new_csv_position)

                await self._flush_due_burst_events(channel)
                await asyncio.sleep(self.poll_interval)
                tick_end = datetime.now(timezone.utc).timestamp()
                loop_delay = tick_end - tick_start
                logger.debug("Watch-Tick fertig in %.3fs", loop_delay)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Fehler in Watch-Loop: %s", exc)
                await asyncio.sleep(max(self.poll_interval, 2.0))

    async def _leaderboard_loop(self) -> None:
        await self.wait_until_ready()
        if self.leaderboard_interval_seconds <= 0:
            logger.info("Automatisches Leaderboard deaktiviert (Intervall <= 0).")
            return

        while not self.is_closed():
            logger.info("Nächster geplante Leaderboard-Post in %ss", self.leaderboard_interval_seconds)
            await asyncio.sleep(self.leaderboard_interval_seconds)
            try:
                if not self.discord_posting_enabled:
                    logger.info("Automatisches Leaderboard uebersprungen (Discord-Posting deaktiviert).")
                    continue
                channel = await self._resolve_channel()
                if channel is None:
                    continue
                embeds = await self._build_leaderboard_embeds("all", requested_by="Automatisch alle 6h")
                if embeds:
                    await channel.send(embeds=embeds)
                    if self.discord_message_debug:
                        logger.info("[%s] channel=%s payload=%s", DISCORD_MESSAGE_LOG_MARKER, channel.id, "leaderboard(all)")
            except Exception as exc:  # noqa: BLE001
                logger.exception("Fehler beim automatischen Leaderboard-Post: %s", exc)

    async def _db_log_loop(self) -> None:
        await self.wait_until_ready()
        if not self.db_discord_log_enabled:
            logger.info("Discord-DB-Telemetrie deaktiviert.")
            return

        logger.info(
            "Discord-DB-Telemetrie aktiv (Intervall=%ss)",
            self.db_discord_log_interval_seconds,
        )
        while not self.is_closed():
            await asyncio.sleep(self.db_discord_log_interval_seconds)
            try:
                metrics = await self.stats_store.pop_db_metrics()
                reads = int(metrics.get("reads", 0))
                writes = int(metrics.get("writes", 0))
                commits = int(metrics.get("commits", 0))
                if reads == 0 and writes == 0 and commits == 0:
                    continue

                payload = (
                    f"DB-Telemetrie ({self.db_discord_log_interval_seconds}s): "
                    f"reads={reads} writes={writes} commits={commits}"
                )
                if self.discord_posting_enabled:
                    channel = await self._resolve_channel()
                    if channel is not None:
                        await channel.send(payload)
                        self._log_discord_payload(channel, payload)
                    continue

                self._log_discord_payload(None, payload, dry_run=True)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Fehler in DB-Telemetrie-Loop: %s", exc)

    def _log_discord_payload(self, channel: discord.TextChannel | None, payload: str, dry_run: bool = False) -> None:
        if not self.log_discord_payloads:
            return
        channel_repr = channel.id if channel is not None else "dry-run"
        marker = f"{DISCORD_MESSAGE_LOG_MARKER}_DRYRUN" if dry_run else DISCORD_MESSAGE_LOG_MARKER
        logger.info("[%s] channel=%s payload=%s", marker, channel_repr, payload)

    def _disambiguate_message_payload(
        self,
        embed: discord.Embed,
        channel: discord.TextChannel | None,
        event: ParsedEvent,
    ) -> str:
        fields = ", ".join(f"{field.name}={field.value}" for field in embed.fields)
        channel_repr = channel.id if channel is not None else "dry-run"
        return (
            f"channel={channel_repr} title={embed.title} description={embed.description} fields=[{fields}] "
            f"footer={embed.footer.text} ts={embed.timestamp.isoformat() if embed.timestamp else ''} rule={event.rule_name}"
        )

    async def _persist_event(self, event: ParsedEvent) -> None:
        ctx = event.context
        if event.rule_name == "player_joined":
            await self.stats_store.record_player_seen(ctx.get("player", ""))
            return

        if event.rule_name == "player_left":
            await self.stats_store.record_player_seen(ctx.get("player", ""))
            return

        if event.rule_name == "pve_tame_completed":
            player = ctx.get("player", "")
            tribe = ctx.get("tribe", "")
            dino = ctx.get("dino", "")
            level_raw = ctx.get("level", "")
            level: int | None = int(level_raw) if level_raw.isdigit() else None
            await self.stats_store.record_dino_tame(player_name=player, dino_type=dino, dino_level=level, tribe_name=tribe)
            return

        if event.rule_name == "pve_dino_killed":
            killer = ctx.get("killer", "")
            dino = ctx.get("dino", "")
            await self.stats_store.record_dino_kill(
                killer_name=killer,
                dino_type=dino,
                event_time_text=ctx.get("logtime", ""),
                source="shootergame_log",
            )
            return

        if event.rule_name == "player_death_by":
            victim = ctx.get("player", "")
            killer = ctx.get("killer", "")
            event_time_text = ctx.get("logtime", "")
            await self.stats_store.record_player_seen(victim)
            await self.stats_store.record_player_death(
                victim_name=victim,
                killer_text=killer,
                event_time_text=event_time_text,
                source_rule=event.rule_name,
            )
            if self._looks_like_player_name(killer):
                await self.stats_store.record_player_kill(killer_name=killer, victim_name=victim)
            return

        if event.rule_name == "player_death_unknown":
            victim = ctx.get("player", "")
            event_time_text = ctx.get("logtime", "")
            await self.stats_store.record_player_seen(victim)
            await self.stats_store.record_player_death(
                victim_name=victim,
                killer_text=None,
                event_time_text=event_time_text,
                source_rule=event.rule_name,
            )
            return

    @staticmethod
    def _looks_like_player_name(killer: str) -> bool:
        normalized = killer.strip().lower()
        if not normalized:
            return False
        if normalized.startswith("a ") or normalized.startswith("an ") or normalized.startswith("the "):
            return False
        if "|" in normalized:
            return False
        return True

    async def _send_immediate_event(self, channel: discord.TextChannel | None, event: ParsedEvent) -> None:
        if event.key in self.recent_events:
            return

        now_ts = datetime.now(timezone.utc).timestamp()
        last_sent_ts = self.last_sent_by_rule.get(event.rule_name, 0.0)
        if event.cooldown_seconds > 0 and (now_ts - last_sent_ts) < event.cooldown_seconds:
            return

        self.recent_events.append(event.key)
        self.last_sent_by_rule[event.rule_name] = now_ts

        embed = self._build_embed(event)
        if self.discord_posting_enabled and channel is not None:
            await channel.send(embed=embed)
            if self.discord_message_debug:
                self._log_discord_payload(channel, self._disambiguate_message_payload(embed, channel, event))
            return

        if self.discord_message_debug:
            self._log_discord_payload(
                None,
                self._disambiguate_message_payload(embed, None, event),
                dry_run=True,
            )

    def _queue_burst_event(self, event: ParsedEvent) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()
        if event.rule_name not in self.pending_burst_events:
            self.pending_burst_events[event.rule_name] = []
            self.pending_burst_started_ts[event.rule_name] = now_ts
            self.pending_burst_window_seconds[event.rule_name] = (
                event.aggregation_window_seconds if event.aggregation_window_seconds > 0 else 30.0
            )

        self.pending_burst_events[event.rule_name].append(event)

    async def _flush_due_burst_events(self, channel: discord.TextChannel | None) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()
        due_rules: list[str] = []
        for rule_name, started_ts in self.pending_burst_started_ts.items():
            window = self.pending_burst_window_seconds.get(rule_name, 30.0)
            if (now_ts - started_ts) >= window:
                due_rules.append(rule_name)

        for rule_name in due_rules:
            events = self.pending_burst_events.pop(rule_name, [])
            started_ts = self.pending_burst_started_ts.pop(rule_name, now_ts)
            self.pending_burst_window_seconds.pop(rule_name, None)
            if not events:
                continue
            await self._send_burst_summary(channel, rule_name, events, started_ts, now_ts)

        for rule_name, events in list(self.pending_burst_events.items()):
            if len(events) < self.burst_max_buffer_size:
                continue
            started_ts = self.pending_burst_started_ts.pop(rule_name, now_ts)
            self.pending_burst_events.pop(rule_name, None)
            self.pending_burst_window_seconds.pop(rule_name, None)
            await self._send_burst_summary(channel, rule_name, events, started_ts, now_ts)

    async def _send_burst_summary(
        self,
        channel: discord.TextChannel | None,
        rule_name: str,
        events: list[ParsedEvent],
        started_ts: float,
        ended_ts: float,
    ) -> None:
        first = events[0]
        counts = Counter(event.aggregate_key for event in events)
        top_items = counts.most_common(max(1, self.burst_top_items))
        top_lines = "\n".join(f"{name} x{count}" for name, count in top_items)

        start_dt = datetime.fromtimestamp(started_ts, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(ended_ts, tz=timezone.utc)
        window_seconds = int(max(1, ended_ts - started_ts))

        embed = discord.Embed(
            title=f"{first.emoji} {first.title} (Burst Summary)",
            description=f"{len(events)} Events in {window_seconds}s",
            color=first.color,
            timestamp=end_dt,
        )
        embed.add_field(name="Top Items", value=top_lines[:1024] if top_lines else "-", inline=False)
        embed.add_field(
            name="Zeitraum (UTC)",
            value=f"{start_dt.strftime('%H:%M:%S')} - {end_dt.strftime('%H:%M:%S')}",
            inline=False,
        )
        embed.set_footer(text=f"ARK Ascended PvPvE Event Feed | {rule_name}")
        if self.discord_posting_enabled and channel is not None:
            await channel.send(embed=embed)
            if self.discord_message_debug:
                self._log_discord_payload(channel, self._disambiguate_message_payload(embed, channel, first))
            return

        if self.discord_message_debug:
            self._log_discord_payload(
                None,
                self._disambiguate_message_payload(embed, None, first),
                dry_run=True,
            )

    async def _build_leaderboard_embeds(self, board: str, requested_by: str) -> list[discord.Embed]:
        kinds: list[str]
        if board == "all":
            kinds = ["dino_kills", "player_kills", "dino_tames"]
        else:
            kinds = [board]

        definitions = {
            "dino_kills": ("Top 5 Dino Kills", "🏹", 0x3498DB),
            "player_kills": ("Top 5 Player Kills", "⚔️", 0xE74C3C),
            "dino_tames": ("Top 5 Dino Tames", "🦖", 0x2ECC71),
        }

        embeds: list[discord.Embed] = []
        for kind in kinds:
            definition = definitions.get(kind)
            if definition is None:
                continue

            title, emoji, color = definition
            rows = await self.stats_store.fetch_top_players(metric=kind, limit=5)
            description = (
                "\n".join(f"`#{idx}` **{name}** - `{score}`" for idx, (name, score) in enumerate(rows, start=1))
                if rows
                else "Noch keine Daten vorhanden."
            )

            embed = discord.Embed(
                title=f"{emoji} {title}",
                description=description,
                color=color,
                timestamp=datetime.now(timezone.utc),
            )
            embed.set_footer(text=f"ARK Leaderboard | {requested_by}")
            if self.discord_message_debug:
                logger.debug(
                    "Leaderboard Embed vorbereitet board=%s requested_by=%s player_count=%s",
                    board,
                    requested_by,
                    len(rows),
                )
            embeds.append(embed)

        return embeds

    def _build_embed(self, event: ParsedEvent) -> discord.Embed:
        embed = discord.Embed(
            title=f"{event.emoji} {event.title}",
            description=event.description,
            color=event.color,
            timestamp=datetime.now(timezone.utc),
        )
        for field_name, field_value in event.fields[:8]:
            embed.add_field(name=field_name, value=field_value, inline=True)
        embed.set_footer(text="ARK Ascended PvPvE Event Feed")
        return embed


def load_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Fehlende Umgebungsvariable: {name}")
    return value


def main() -> None:
    load_dotenv()
    configure_logging()

    token = load_required_env("DISCORD_TOKEN")
    channel_id = int(load_required_env("DISCORD_CHANNEL_ID"))
    log_path = Path(load_required_env("ARK_LOG_PATH"))
    wild_kill_csv_path_raw = os.getenv("ARK_WILD_KILLS_CSV_PATH", "").strip()
    rules_path = Path(os.getenv("ARK_RULES_PATH", "rules.json"))
    db_path = Path(os.getenv("ARK_DB_PATH", "ark_stats.db"))

    poll_interval = float(os.getenv("POLL_INTERVAL_SECONDS", "1.5"))
    hard_reopen_interval_seconds = float(os.getenv("ARK_LOG_HARD_REOPEN_INTERVAL_SECONDS", "900"))
    burst_top_items = int(os.getenv("BURST_TOP_ITEMS", "5"))
    burst_max_buffer_size = int(os.getenv("BURST_MAX_BUFFER_SIZE", "250"))
    leaderboard_interval_seconds = int(os.getenv("LEADERBOARD_POST_INTERVAL_SECONDS", "21600"))
    discord_posting_enabled = _env_bool("ARK_DISCORD_POSTING_ENABLED", True)
    db_discord_log_enabled = _env_bool("ARK_DB_DISCORD_LOG_ENABLED", False)
    db_discord_log_interval_seconds = int(os.getenv("ARK_DB_DISCORD_LOG_INTERVAL_SECONDS", "300"))
    wild_kill_feature_enabled = _env_bool("ARK_WILD_KILLS_FEATURE_ENABLED", False)

    rule_engine = RuleEngine(rules_path=rules_path)
    tail = LogTail(path=log_path, hard_reopen_interval_seconds=hard_reopen_interval_seconds)
    wild_kill_csv_tail: WildKillCsvTail | None = None
    if wild_kill_feature_enabled and wild_kill_csv_path_raw:
        wild_kill_csv_tail = WildKillCsvTail(path=Path(wild_kill_csv_path_raw))
    elif wild_kill_feature_enabled:
        logger.warning("WildKill Feature ist aktiviert, aber ARK_WILD_KILLS_CSV_PATH ist leer.")
    stats_store = StatsStore(db_path=db_path)

    bot = ArkLogBot(
        channel_id=channel_id,
        rule_engine=rule_engine,
        tail=tail,
        wild_kill_csv_tail=wild_kill_csv_tail,
        wild_kill_feature_enabled=wild_kill_feature_enabled,
        stats_store=stats_store,
        poll_interval=poll_interval,
        burst_top_items=burst_top_items,
        burst_max_buffer_size=burst_max_buffer_size,
        leaderboard_interval_seconds=leaderboard_interval_seconds,
        discord_posting_enabled=discord_posting_enabled,
        db_discord_log_enabled=db_discord_log_enabled,
        db_discord_log_interval_seconds=db_discord_log_interval_seconds,
    )

    bot.run(token)


if __name__ == "__main__":
    main()
