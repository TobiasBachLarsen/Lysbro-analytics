"""
Generates realistic Lysbro platform data and stores it in a local SQLite database.
This simulates what an ETL extract phase would produce from the production database.

Invariants the generator guarantees (covered by tests/test_generate_data.py):
  * a meeting is never scheduled before its host signed up
  * a user is never invited to a meeting scheduled before they signed up
  * meetings.participant_count always equals the number of participant rows
"""

import logging
import random
import sqlite3
from datetime import datetime, timedelta

from config import (
    DATE_END,
    DATE_START,
    MEETING_DURATIONS,
    MEETING_TITLES,
    MESSAGE_COUNT,
    PLAN_MEETING_RANGE,
    PLAN_PARTICIPANT_RANGE,
    PLAN_WEIGHTS,
    PLANS,
    RANDOM_SEED,
    RSVP_STATES,
    RSVP_WEIGHTS,
    SOURCE_DB,
)

log = logging.getLogger(__name__)

NAMES = [
    "Anders Nielsen",
    "Sofie Hansen",
    "Mikkel Andersen",
    "Laura Jensen",
    "Jonas Christensen",
    "Emma Pedersen",
    "Oliver Larsen",
    "Isabella Møller",
    "Noah Thomsen",
    "Maja Kristensen",
    "Lucas Rasmussen",
    "Freja Johansen",
    "William Poulsen",
    "Astrid Madsen",
    "Oscar Jørgensen",
    "Clara Olsen",
    "Elias Sørensen",
    "Anna Petersen",
    "Magnus Nielsen",
    "Sara Knudsen",
    "Tobias Lund",
    "Ida Mortensen",
    "Sebastian Dahl",
    "Nora Berg",
    "Alexander Holm",
    "Emilie Jakobsen",
    "Victor Eriksen",
    "Mathilde Simonsen",
]

SCHEMA = """
    DROP TABLE IF EXISTS messages;
    DROP TABLE IF EXISTS meeting_participants;
    DROP TABLE IF EXISTS meetings;
    DROP TABLE IF EXISTS users;

    CREATE TABLE users (
        id          INTEGER PRIMARY KEY,
        name        TEXT    NOT NULL,
        email       TEXT    NOT NULL UNIQUE,
        plan        TEXT    NOT NULL CHECK(plan IN ('gratis', 'pro', 'erhverv')),
        created_at  TEXT    NOT NULL
    );

    CREATE TABLE meetings (
        id                INTEGER PRIMARY KEY,
        host_id           INTEGER NOT NULL REFERENCES users(id),
        title             TEXT    NOT NULL,
        scheduled_at      TEXT    NOT NULL,
        duration_min      INTEGER NOT NULL CHECK(duration_min > 0),
        participant_count INTEGER NOT NULL CHECK(participant_count >= 0)
    );

    CREATE TABLE meeting_participants (
        meeting_id  INTEGER NOT NULL REFERENCES meetings(id),
        user_id     INTEGER NOT NULL REFERENCES users(id),
        rsvp        TEXT    NOT NULL CHECK(rsvp IN ('accepted', 'pending', 'declined')),
        PRIMARY KEY (meeting_id, user_id)
    );

    CREATE TABLE messages (
        id          INTEGER PRIMARY KEY,
        sender_id   INTEGER NOT NULL REFERENCES users(id),
        receiver_id INTEGER NOT NULL REFERENCES users(id),
        sent_at     TEXT    NOT NULL,
        CHECK(sender_id != receiver_id)
    );
"""


def random_date(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, delta_seconds))


def _seed_users(
    db: sqlite3.Connection, rng: random.Random, start: datetime, end: datetime
) -> list[dict]:
    users = []
    for user_id, name in enumerate(NAMES, start=1):
        email = name.lower().replace(" ", ".") + "@example.dk"
        plan = rng.choices(PLANS, PLAN_WEIGHTS)[0]
        created_at = random_date(rng, start, end)
        db.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            (user_id, name, email, plan, created_at.isoformat()),
        )
        users.append({"id": user_id, "plan": plan, "created_at": created_at})
    return users


def _seed_meetings(
    db: sqlite3.Connection, rng: random.Random, users: list[dict], end: datetime
) -> int:
    meeting_id = 0
    for host in users:
        lo, hi = PLAN_MEETING_RANGE[host["plan"]]
        p_lo, p_hi = PLAN_PARTICIPANT_RANGE[host["plan"]]

        for _ in range(rng.randint(lo, hi)):
            meeting_id += 1
            scheduled_at = random_date(rng, host["created_at"], end)

            # Only users who already existed at the time of the meeting can be invited.
            eligible = [
                u["id"] for u in users if u["id"] != host["id"] and u["created_at"] <= scheduled_at
            ]
            participants = rng.sample(eligible, min(rng.randint(p_lo, p_hi), len(eligible)))

            db.execute(
                "INSERT INTO meetings VALUES (?, ?, ?, ?, ?, ?)",
                (
                    meeting_id,
                    host["id"],
                    rng.choice(MEETING_TITLES),
                    scheduled_at.isoformat(),
                    rng.choice(MEETING_DURATIONS),
                    len(participants),
                ),
            )
            db.executemany(
                "INSERT INTO meeting_participants VALUES (?, ?, ?)",
                [
                    (meeting_id, pid, rng.choices(RSVP_STATES, RSVP_WEIGHTS)[0])
                    for pid in participants
                ],
            )
    return meeting_id


def _seed_messages(
    db: sqlite3.Connection, rng: random.Random, users: list[dict], end: datetime
) -> int:
    for msg_id in range(1, MESSAGE_COUNT + 1):
        sender, receiver = rng.sample(users, 2)
        earliest = max(sender["created_at"], receiver["created_at"])
        db.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?)",
            (msg_id, sender["id"], receiver["id"], random_date(rng, earliest, end).isoformat()),
        )
    return MESSAGE_COUNT


def seed(db: sqlite3.Connection, rng: random.Random, start: datetime, end: datetime) -> None:
    """Drop and recreate all source tables, then fill them with synthetic data."""
    db.executescript(SCHEMA)
    users = _seed_users(db, rng, start, end)
    n_meetings = _seed_meetings(db, rng, users, end)
    n_messages = _seed_messages(db, rng, users, end)
    db.commit()
    log.info("Seeded %d users, %d meetings, %d messages", len(users), n_meetings, n_messages)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    SOURCE_DB.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(SOURCE_DB) as db:
        seed(
            db,
            rng=random.Random(RANDOM_SEED),
            start=datetime.fromisoformat(DATE_START),
            end=datetime.fromisoformat(DATE_END),
        )
    log.info("Source database written to %s", SOURCE_DB)


if __name__ == "__main__":
    main()
