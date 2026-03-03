import asyncio
import json
import logging
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import discord
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ark-log-bot")


@dataclass
class ParsedEvent:
    rule_name: str
    key: str
    title: str
    description: str
    color: int
    emoji: str
    fields: list[tuple[str, str]]
    cooldown_seconds: float


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
                title=title,
                description=description,
                color=rule["color"],
                emoji=rule["emoji"],
                fields=fields,
                cooldown_seconds=rule["cooldown_seconds"],
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
        self.path = path
        self.position = 0

    def read_new_lines(self) -> list[str]:
        if not self.path.exists():
            return []

        current_size = self.path.stat().st_size

        # Bei Log-Rotation/Truncate wieder am Anfang starten.
        if current_size < self.position:
            self.position = 0

        lines: list[str] = []
        with self.path.open("r", encoding="utf-8", errors="replace") as f:
            f.seek(self.position)
            chunk = f.read()
            self.position = f.tell()

        if not chunk:
            return lines

        for line in chunk.splitlines():
            cleaned = line.strip()
            if cleaned:
                lines.append(cleaned)

        return lines


class ArkLogBot(discord.Client):
    def __init__(
        self,
        channel_id: int,
        rule_engine: RuleEngine,
        tail: LogTail,
        poll_interval: float,
    ):
        intents = discord.Intents.none()
        super().__init__(intents=intents)
        self.channel_id = channel_id
        self.rule_engine = rule_engine
        self.tail = tail
        self.poll_interval = poll_interval
        self.recent_events = deque(maxlen=200)
        self.last_sent_by_rule: dict[str, float] = {}
        self.bg_task: asyncio.Task | None = None

    async def on_ready(self) -> None:
        logger.info("Bot eingeloggt als %s (%s)", self.user, self.user.id if self.user else "?")
        if self.bg_task is None:
            self.bg_task = asyncio.create_task(self._watch_loop(), name="log-watch-loop")

    async def _watch_loop(self) -> None:
        await self.wait_until_ready()
        channel = self.get_channel(self.channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(self.channel_id)
            except discord.NotFound:
                logger.error(
                    "Channel %s nicht gefunden. Bitte pruefe, ob es wirklich eine Textchannel-ID ist.",
                    self.channel_id,
                )
                return
            except discord.Forbidden:
                logger.error(
                    "Kein Zugriff auf Channel %s. Bitte pruefe Bot-Rechte und Channel-Overrides.",
                    self.channel_id,
                )
                return
            except discord.HTTPException as exc:
                logger.error("Discord API Fehler beim Laden von Channel %s: %s", self.channel_id, exc)
                return

        if not isinstance(channel, discord.TextChannel):
            logger.error("Channel %s ist kein TextChannel.", self.channel_id)
            return

        logger.info("Starte Log-Watcher für %s", self.tail.path)

        while not self.is_closed():
            try:
                new_lines = self.tail.read_new_lines()
                for line in new_lines:
                    event = self.rule_engine.parse_line(line)
                    if event is None:
                        continue

                    if event.key in self.recent_events:
                        continue

                    now_ts = datetime.now(timezone.utc).timestamp()
                    last_sent_ts = self.last_sent_by_rule.get(event.rule_name, 0.0)
                    if event.cooldown_seconds > 0 and (now_ts - last_sent_ts) < event.cooldown_seconds:
                        continue

                    self.recent_events.append(event.key)
                    self.last_sent_by_rule[event.rule_name] = now_ts

                    embed = self._build_embed(event)
                    await channel.send(embed=embed)

                await asyncio.sleep(self.poll_interval)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Fehler in Watch-Loop: %s", exc)
                await asyncio.sleep(max(self.poll_interval, 2.0))

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
    poll_interval = float(os.getenv("POLL_INTERVAL_SECONDS", "1.5"))

    rule_engine = RuleEngine(rules_path=rules_path)
    tail = LogTail(path=log_path)

    bot = ArkLogBot(
        channel_id=channel_id,
        rule_engine=rule_engine,
        tail=tail,
        poll_interval=poll_interval,
    )

    bot.run(token)


if __name__ == "__main__":
    main()
