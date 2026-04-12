import asyncio
from pathlib import Path

from bot import ArkLogBot, RuleEngine, StatsStore, WildKillCsvTail


def run(coro):
    return asyncio.run(coro)


def test_rule_engine_matches_player_death_with_tribe_name():
    rules_path = Path(__file__).resolve().parents[1] / "rules.json"
    engine = RuleEngine(rules_path=rules_path)

    line = (
        "[2026.03.26-20.37.05:603][123]2026.03.26_20.37.05: "
        "Ollinator - Lvl 48 (Pulpinesen) was killed by a Raptor!"
    )
    event = engine.parse_line(line)

    assert event is not None
    assert event.rule_name == "player_death_by"
    assert event.context["player"] == "Ollinator"
    assert event.context["killer"] == "a Raptor"


def test_rule_engine_matches_server_started_successfully_line():
    rules_path = Path(__file__).resolve().parents[1] / "rules.json"
    engine = RuleEngine(rules_path=rules_path)

    line = (
        "[2026.03.29-06.38.10:510][  3]Server: "
        "\"Pulpinesien|PVPVE|Leaderboards|MultiplierActive\" has successfully started!"
    )
    event = engine.parse_line(line)

    assert event is not None
    assert event.rule_name == "server_started"


def test_rule_engine_matches_ingame_chat_line():
    rules_path = Path(__file__).resolve().parents[1] / "rules.json"
    engine = RuleEngine(rules_path=rules_path)

    line = "[2026.04.12-11.15.00:000][123]Global Chat from 'Ollinator': Hallo zusammen"
    event = engine.parse_line(line)

    assert event is not None
    assert event.rule_name == "ingame_chat_message"
    assert event.context["player"] == "Ollinator"
    assert event.context["channel"] == "Global"
    assert event.context["message"] == "Hallo zusammen"


def test_wild_kill_csv_tail_parses_rows_and_skips_repeated_header(tmp_path):
    csv_path = tmp_path / "wild_kills.csv"
    csv_path.write_text(
        "timestamp_utc,dino_blueprint,dino_x,dino_y,dino_z,killer_eos,killer_name,nearest_distance\n"
        "\"2026-03-22T12:42:20Z\",\"Blueprint'/Game/PrimalEarth/Dinos/Raptor/Raptor_Character_BP.Raptor_Character_BP'\",1,2,3,\"id\",\"Ollinator\",10\n"
        "timestamp_utc,dino_blueprint,dino_x,dino_y,dino_z,killer_eos,killer_name,nearest_distance,targeting_team,original_targeting_team\n"
        "\"2026-03-22T12:43:20Z\",\"Blueprint'/Game/PrimalEarth/Dinos/Dodo/Dodo_Character_BP.Dodo_Character_BP'\",1,2,3,\"id\",\"Ollinator\",10,1,1\n",
        encoding="utf-8",
    )

    tail = WildKillCsvTail(csv_path)
    kills, _ = tail.read_new_kills()

    assert len(kills) == 2
    assert kills[0] == ("Ollinator", "Raptor", "2026-03-22T12:42:20Z")
    assert kills[1] == ("Ollinator", "Dodo", "2026-03-22T12:43:20Z")


def test_record_player_death_normalizes_player_name(tmp_path):
    db_path = tmp_path / "test.db"
    store = StatsStore(db_path=db_path)

    run(
        store.record_player_death(
            victim_name="Ollinator - Lvl 47 (Pulpinesen) was",
            killer_text="a Raptor",
            event_time_text="2026.03.26-20.37.05:603",
            source_rule="player_death_by",
        )
    )

    row = store.conn.execute(
        "SELECT victim_name, killer_text, source_rule FROM player_death_events LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row["victim_name"] == "Ollinator"
    assert row["killer_text"] == "a Raptor"
    assert row["source_rule"] == "player_death_by"

    player = store.conn.execute("SELECT player_name FROM players LIMIT 1").fetchone()
    assert player is not None
    assert player["player_name"] == "Ollinator"


def test_normalization_migrates_and_merges_existing_level_based_duplicates(tmp_path):
    db_path = tmp_path / "test.db"
    store = StatsStore(db_path=db_path)

    now = "2026-03-26T20:00:00+00:00"
    store.conn.execute(
        "INSERT INTO players (player_name, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
        ("Ollinator - Lvl 45 (Pulpinesen) was", now, now),
    )
    store.conn.execute(
        "INSERT INTO players (player_name, first_seen_at, last_seen_at) VALUES (?, ?, ?)",
        ("Ollinator - Lvl 48 (Pulpinesen) was", now, now),
    )
    p1 = store.conn.execute(
        "SELECT id FROM players WHERE player_name = ?",
        ("Ollinator - Lvl 45 (Pulpinesen) was",),
    ).fetchone()["id"]
    p2 = store.conn.execute(
        "SELECT id FROM players WHERE player_name = ?",
        ("Ollinator - Lvl 48 (Pulpinesen) was",),
    ).fetchone()["id"]
    store.conn.execute(
        "INSERT INTO player_stats (player_id, dino_kills_total, player_kills_total, dino_tames_total, updated_at) VALUES (?, ?, 0, 0, ?)",
        (p1, 2, now),
    )
    store.conn.execute(
        "INSERT INTO player_stats (player_id, dino_kills_total, player_kills_total, dino_tames_total, updated_at) VALUES (?, ?, 0, 0, ?)",
        (p2, 3, now),
    )
    store.conn.commit()

    store._normalize_existing_player_rows()

    players = store.conn.execute("SELECT player_name FROM players ORDER BY player_name").fetchall()
    assert len(players) == 1
    assert players[0]["player_name"] == "Ollinator"

    stats = store.conn.execute("SELECT dino_kills_total FROM player_stats").fetchall()
    assert len(stats) == 1
    assert stats[0]["dino_kills_total"] == 5


def test_record_and_fetch_last_dino_kill_uses_normalized_player_name(tmp_path):
    db_path = tmp_path / "test.db"
    store = StatsStore(db_path=db_path)

    run(
        store.record_dino_kill(
            killer_name="Ollinator - Lvl 48 (Pulpinesen) was",
            dino_type="Raptor",
            event_time_text="2026.03.26-20.37.05:603",
            source="shootergame_log",
        )
    )

    result = run(store.fetch_last_dino_kill_for_player("Ollinator"))
    assert result is not None
    player_name, dino_type, event_time, source = result
    assert player_name == "Ollinator"
    assert dino_type == "Raptor"
    assert source == "shootergame_log"
    assert event_time == "2026.03.26-20.37.05:603"


def test_record_dino_kill_normalizes_leveled_dino_killer_to_name_and_species(tmp_path):
    db_path = tmp_path / "test.db"
    store = StatsStore(db_path=db_path)

    run(
        store.record_dino_kill(
            killer_name="Dilli - Lvl 21 (Dilophosaur) (Pulpinesen) was",
            dino_type="Dodo",
            event_time_text="2026.03.27-10.00.00:000",
            source="shootergame_log",
        )
    )

    player = store.conn.execute("SELECT player_name FROM players LIMIT 1").fetchone()
    assert player is not None
    assert player["player_name"] == "Dilli (Dilophosaur)"


def test_sanitize_outgoing_chat_message_removes_newlines_and_trims():
    cleaned = ArkLogBot._sanitize_outgoing_chat_message("  Hallo\nWelt\r!  ", 10)
    assert cleaned == "Hallo Welt"
