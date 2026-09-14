import random
import sqlite3
from datetime import datetime

import pandas as pd

import generate_data


def _seeded_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    generate_data.seed(
        db,
        rng=random.Random(1),
        start=datetime(2025, 1, 1),
        end=datetime(2025, 12, 31),
    )
    return db


def test_participant_count_matches_participant_rows():
    db = _seeded_db()
    mismatches = pd.read_sql(
        """
        SELECT COUNT(*) AS n FROM meetings m
        WHERE participant_count != (
            SELECT COUNT(*) FROM meeting_participants p WHERE p.meeting_id = m.id
        )
        """,
        db,
    )["n"][0]
    assert mismatches == 0


def test_nobody_is_invited_before_they_signed_up():
    db = _seeded_db()
    early = pd.read_sql(
        """
        SELECT COUNT(*) AS n
        FROM meeting_participants p
        JOIN meetings m ON m.id = p.meeting_id
        JOIN users u ON u.id = p.user_id
        WHERE m.scheduled_at < u.created_at
        """,
        db,
    )["n"][0]
    assert early == 0


def test_no_meeting_is_scheduled_before_its_host_signed_up():
    db = _seeded_db()
    early = pd.read_sql(
        """
        SELECT COUNT(*) AS n FROM meetings m
        JOIN users u ON u.id = m.host_id
        WHERE m.scheduled_at < u.created_at
        """,
        db,
    )["n"][0]
    assert early == 0


def test_seed_is_deterministic():
    a = pd.read_sql("SELECT * FROM meetings", _seeded_db())
    b = pd.read_sql("SELECT * FROM meetings", _seeded_db())
    pd.testing.assert_frame_equal(a, b)
