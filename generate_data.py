"""
Generates realistic Lysbro platform data and stores it in a local SQLite database.
This simulates what an ETL extract phase would produce from the production database.
"""

import random
import sqlite3
from datetime import datetime, timedelta

from config import (
    DATE_END,
    DATE_START,
    MEETING_DURATIONS,
    MEETING_TITLES,
    PLAN_MEETING_RANGE,
    PLAN_PARTICIPANT_RANGE,
    PLAN_WEIGHTS,
    PLANS,
    RANDOM_SEED,
    SOURCE_DB as DB_PATH,
)

NAMES = [
    "Anders Nielsen", "Sofie Hansen", "Mikkel Andersen", "Laura Jensen",
    "Jonas Christensen", "Emma Pedersen", "Oliver Larsen", "Isabella Møller",
    "Noah Thomsen", "Maja Kristensen", "Lucas Rasmussen", "Freja Johansen",
    "William Poulsen", "Astrid Madsen", "Oscar Jørgensen", "Clara Olsen",
    "Elias Sørensen", "Anna Petersen", "Magnus Nielsen", "Sara Knudsen",
    "Tobias Lund", "Ida Mortensen", "Sebastian Dahl", "Nora Berg",
    "Alexander Holm", "Emilie Jakobsen", "Victor Eriksen", "Mathilde Simonsen",
]


def random_date(start: datetime, end: datetime) -> datetime:
    delta_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta_seconds))


def _create_schema(db: sqlite3.Connection) -> None:
    db.executescript("""
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
    """)


def seed(db: sqlite3.Connection, start: datetime, end: datetime) -> None:
    _create_schema(db)

    # ── Users ─────────────────────────────────────────────────────────────────
    users: list[dict] = []
    for i, name in enumerate(NAMES, start=1):
        email = name.lower().replace(" ", ".") + "@example.dk"
        plan = random.choices(PLANS, PLAN_WEIGHTS)[0]
        created_at = random_date(start, end).isoformat()
        db.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
            (i, name, email, plan, created_at),
        )
        users.append({"id": i, "plan": plan, "created_at": created_at})

    all_user_ids = [u["id"] for u in users]

    # ── Meetings ──────────────────────────────────────────────────────────────
    meeting_id = 1
    for user in users:
        lo, hi = PLAN_MEETING_RANGE[user["plan"]]
        p_lo, p_hi = PLAN_PARTICIPANT_RANGE[user["plan"]]
        host_start = datetime.fromisoformat(user["created_at"])
        other_ids = [uid for uid in all_user_ids if uid != user["id"]]

        for _ in range(random.randint(lo, hi)):
            n_participants = random.randint(p_lo, p_hi)
            db.execute(
                "INSERT INTO meetings VALUES (?, ?, ?, ?, ?, ?)",
                (
                    meeting_id,
                    user["id"],
                    random.choice(MEETING_TITLES),
                    random_date(host_start, end).isoformat(),
                    random.choice(MEETING_DURATIONS),
                    n_participants,
                ),
            )
            for pid in random.sample(other_ids, min(n_participants, len(other_ids))):
                rsvp = random.choices(
                    ["accepted", "pending", "declined"], [0.6, 0.3, 0.1]
                )[0]
                db.execute(
                    "INSERT OR IGNORE INTO meeting_participants VALUES (?, ?, ?)",
                    (meeting_id, pid, rsvp),
                )
            meeting_id += 1

    # ── Messages ──────────────────────────────────────────────────────────────
    for msg_id in range(1, 501):
        sender, receiver = random.sample(users, 2)
        db.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?)",
            (msg_id, sender["id"], receiver["id"], random_date(start, end).isoformat()),
        )

    db.commit()
    print(f"Seeded {len(users)} users, {meeting_id - 1} meetings, 500 messages → {DB_PATH}")


if __name__ == "__main__":
    random.seed(RANDOM_SEED)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as db:
        seed(db, start=datetime.fromisoformat(DATE_START), end=datetime.fromisoformat(DATE_END))
