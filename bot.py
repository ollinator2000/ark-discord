import asyncio
import json
import logging
import os
import re
import sqlite3
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import discord
from discord import app_commands
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ark-log-bot")


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

            event_key = f"{rule['name']}|{description}"
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
    def __init__(self, path: Path):
        self.configured_path = path
        self.path = self._resolve_current_file()
        self.position = 0
        self._silent_poll_count = 0
        self._max_silent_polls_before_switch = 12
        self._active_file_key: tuple[int, int] | None = None
        self._active_path: Path | None = None
        self._last_active_log_report = 0.0
        self._path_prefixes = self._build_prefixes(self.configured_path.stem)
        if self.path is not None:
            try:
                st = self.path.stat()
                self._active_path = self.path
                self._active_file_key = (st.st_dev, st.st_ino)
            except OSError:
                self.path = None

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
    def _is_log_file(path: Path) -> bool:
        return path.is_file() and path.suffix.lower() == ".log"

    def _candidate_files(self) -> list[Path]:
        candidates: list[Path] = []
        if self.configured_path.exists():
            candidates.append(self.configured_path)

        parent = self.configured_path.parent
        if not parent.exists() or not parent.is_dir():
            return candidates

        all_logs = [path for path in parent.glob("*.log") if path.is_file()]
        if self._path_prefixes:
            preferred = [
                path for path in all_logs if any(path.name.startswith(prefix) for prefix in self._path_prefixes)
            ]
            if preferred:
                candidates.extend(preferred)
            elif candidates:
                # keep configured file if it exists; ignore unmatched logs
                pass
            else:
                candidates.extend(all_logs)
        else:
            candidates.extend(all_logs)

        deduped = {str(path.resolve()): path for path in candidates if self._is_log_file(path)}
        return sorted(deduped.values(), key=lambda p: (p.stat().st_mtime_ns, p.name))

    def _resolve_current_file(self, allow_stale_active: bool = True) -> Path | None:
        candidates = self._candidate_files()
        if not candidates:
            return None

        if (
            allow_stale_active
            and self._active_path is not None
            and self._active_path in candidates
            and self._is_file_active(self._active_path)
        ):
            return self._active_path

        return candidates[-1]

    @staticmethod
    def _is_file_active(path: Path) -> bool:
        try:
            age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
        except OSError:
            return False

        return age_seconds < 45.0

    def _read_from_path(self, path: Path) -> tuple[list[str], int]:
        lines: list[str] = []
        with path.open("r", encoding="utf-8", errors="replace") as f:
            f.seek(self.position)
            chunk = f.read()
            new_position = f.tell()

        if not chunk:
            return lines, new_position

        for line in chunk.splitlines():
            cleaned = line.strip()
            if cleaned:
                lines.append(cleaned)

        return lines, new_position

    def read_new_lines(self) -> list[str]:
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


class StatsStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
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

            CREATE INDEX IF NOT EXISTS idx_players_name ON players(player_name);
            CREATE INDEX IF NOT EXISTS idx_tribes_name ON tribes(tribe_name);
            CREATE INDEX IF NOT EXISTS idx_dino_tame_player ON dino_tame_events(player_id);
            CREATE INDEX IF NOT EXISTS idx_player_kill_killer ON player_kill_events(killer_player_id);
            """
        )
        self.conn.commit()

    async def record_player_seen(self, player_name: str) -> int | None:
        normalized = player_name.strip()
        if not normalized:
            return None

        async with self._lock:
            now = utc_now_iso()
            self.conn.execute(
                """
                INSERT INTO players (player_name, first_seen_at, last_seen_at)
                VALUES (?, ?, ?)
                ON CONFLICT(player_name) DO UPDATE SET last_seen_at=excluded.last_seen_at
                """,
                (normalized, now, now),
            )
            self.conn.commit()
            return self._get_player_id_locked(normalized)

    async def link_player_to_tribe(self, player_name: str, tribe_name: str) -> None:
        normalized_player = player_name.strip()
        normalized_tribe = tribe_name.strip()
        if not normalized_player or not normalized_tribe:
            return

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
            self.conn.commit()

    async def record_dino_kill(self, killer_name: str, dino_type: str) -> None:
        killer = killer_name.strip()
        dino = dino_type.strip()
        if not killer or not dino:
            return

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
            self.conn.commit()

    async def record_dino_tame(self, player_name: str, dino_type: str, dino_level: int | None, tribe_name: str) -> None:
        player = player_name.strip()
        dino = dino_type.strip()
        tribe = tribe_name.strip()
        if not player or not dino:
            return

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
            self.conn.commit()

    async def record_player_kill(self, killer_name: str, victim_name: str) -> None:
        killer = killer_name.strip()
        victim = victim_name.strip()
        if not killer:
            return

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
            self.conn.commit()

    async def fetch_top_players(self, metric: str, limit: int = 5) -> list[tuple[str, int]]:
        metric_column_map = {
            "dino_kills": "dino_kills_total",
            "player_kills": "player_kills_total",
            "dino_tames": "dino_tames_total",
        }
        column = metric_column_map.get(metric)
        if column is None:
            return []

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
            return [(str(row["player_name"]), int(row["score"])) for row in rows]

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
        stats_store: StatsStore,
        poll_interval: float,
        burst_top_items: int,
        burst_max_buffer_size: int,
        leaderboard_interval_seconds: int,
    ):
        intents = discord.Intents.none()
        super().__init__(intents=intents)
        self.channel_id = channel_id
        self.rule_engine = rule_engine
        self.tail = tail
        self.stats_store = stats_store
        self.poll_interval = poll_interval
        self.burst_top_items = burst_top_items
        self.burst_max_buffer_size = burst_max_buffer_size
        self.leaderboard_interval_seconds = leaderboard_interval_seconds

        self.recent_events = deque(maxlen=200)
        self.last_sent_by_rule: dict[str, float] = {}
        self.pending_burst_events: dict[str, list[ParsedEvent]] = {}
        self.pending_burst_started_ts: dict[str, float] = {}
        self.pending_burst_window_seconds: dict[str, float] = {}

        self.bg_task: asyncio.Task | None = None
        self.leaderboard_task: asyncio.Task | None = None

        self.tree = app_commands.CommandTree(self)
        self._commands_registered = False

    async def setup_hook(self) -> None:
        self._register_commands()
        await self.tree.sync()
        logger.info("Slash Commands synchronisiert.")

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
            if interaction.channel is None:
                await interaction.response.send_message("Kein gueltiger Channel.", ephemeral=True)
                return

            embeds = await self._build_leaderboard_embeds(board.value, requested_by="On-Demand")
            if not embeds:
                await interaction.response.send_message("Keine Daten fuer dieses Leaderboard vorhanden.")
                return

            await interaction.response.send_message(embeds=embeds)

        self._commands_registered = True

    async def on_ready(self) -> None:
        logger.info("Bot eingeloggt als %s (%s)", self.user, self.user.id if self.user else "?")
        if self.bg_task is None:
            self.bg_task = asyncio.create_task(self._watch_loop(), name="log-watch-loop")
        if self.leaderboard_task is None:
            self.leaderboard_task = asyncio.create_task(self._leaderboard_loop(), name="leaderboard-loop")

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
        channel = await self._resolve_channel()
        if channel is None:
            return

        logger.info("Starte Log-Watcher fuer %s", self.tail.path)

        while not self.is_closed():
            try:
                new_lines = self.tail.read_new_lines()
                for line in new_lines:
                    event = self.rule_engine.parse_line(line)
                    if event is None:
                        continue

                    await self._persist_event(event)

                    if event.event_class == "burst":
                        self._queue_burst_event(event)
                        continue

                    await self._send_immediate_event(channel, event)

                await self._flush_due_burst_events(channel)
                await asyncio.sleep(self.poll_interval)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Fehler in Watch-Loop: %s", exc)
                await asyncio.sleep(max(self.poll_interval, 2.0))

    async def _leaderboard_loop(self) -> None:
        await self.wait_until_ready()
        if self.leaderboard_interval_seconds <= 0:
            logger.info("Automatisches Leaderboard deaktiviert (Intervall <= 0).")
            return

        while not self.is_closed():
            await asyncio.sleep(self.leaderboard_interval_seconds)
            try:
                channel = await self._resolve_channel()
                if channel is None:
                    continue
                embeds = await self._build_leaderboard_embeds("all", requested_by="Automatisch alle 6h")
                if embeds:
                    await channel.send(embeds=embeds)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Fehler beim automatischen Leaderboard-Post: %s", exc)

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
            await self.stats_store.record_dino_kill(killer_name=killer, dino_type=dino)
            return

        if event.rule_name == "player_death_by":
            victim = ctx.get("player", "")
            killer = ctx.get("killer", "")
            await self.stats_store.record_player_seen(victim)
            if self._looks_like_player_name(killer):
                await self.stats_store.record_player_kill(killer_name=killer, victim_name=victim)

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

    async def _send_immediate_event(self, channel: discord.TextChannel, event: ParsedEvent) -> None:
        if event.key in self.recent_events:
            return

        now_ts = datetime.now(timezone.utc).timestamp()
        last_sent_ts = self.last_sent_by_rule.get(event.rule_name, 0.0)
        if event.cooldown_seconds > 0 and (now_ts - last_sent_ts) < event.cooldown_seconds:
            return

        self.recent_events.append(event.key)
        self.last_sent_by_rule[event.rule_name] = now_ts

        embed = self._build_embed(event)
        await channel.send(embed=embed)

    def _queue_burst_event(self, event: ParsedEvent) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()
        if event.rule_name not in self.pending_burst_events:
            self.pending_burst_events[event.rule_name] = []
            self.pending_burst_started_ts[event.rule_name] = now_ts
            self.pending_burst_window_seconds[event.rule_name] = (
                event.aggregation_window_seconds if event.aggregation_window_seconds > 0 else 30.0
            )

        self.pending_burst_events[event.rule_name].append(event)

    async def _flush_due_burst_events(self, channel: discord.TextChannel) -> None:
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
        channel: discord.TextChannel,
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
        await channel.send(embed=embed)

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

    token = load_required_env("DISCORD_TOKEN")
    channel_id = int(load_required_env("DISCORD_CHANNEL_ID"))
    log_path = Path(load_required_env("ARK_LOG_PATH"))
    rules_path = Path(os.getenv("ARK_RULES_PATH", "rules.json"))
    db_path = Path(os.getenv("ARK_DB_PATH", "ark_stats.db"))

    poll_interval = float(os.getenv("POLL_INTERVAL_SECONDS", "1.5"))
    burst_top_items = int(os.getenv("BURST_TOP_ITEMS", "5"))
    burst_max_buffer_size = int(os.getenv("BURST_MAX_BUFFER_SIZE", "250"))
    leaderboard_interval_seconds = int(os.getenv("LEADERBOARD_POST_INTERVAL_SECONDS", "21600"))

    rule_engine = RuleEngine(rules_path=rules_path)
    tail = LogTail(path=log_path)
    stats_store = StatsStore(db_path=db_path)

    bot = ArkLogBot(
        channel_id=channel_id,
        rule_engine=rule_engine,
        tail=tail,
        stats_store=stats_store,
        poll_interval=poll_interval,
        burst_top_items=burst_top_items,
        burst_max_buffer_size=burst_max_buffer_size,
        leaderboard_interval_seconds=leaderboard_interval_seconds,
    )

    bot.run(token)


if __name__ == "__main__":
    main()
