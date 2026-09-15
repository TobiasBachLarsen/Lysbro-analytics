"""
ETL pipeline — extracts raw Lysbro data, transforms it into analytics-ready
aggregates, and loads results into a separate reporting database.

The extract reads the live Lysbro Supabase database when DATABASE_URL is set
(see extract_supabase), and otherwise falls back to the local synthetic SQLite
source so the pipeline still runs self-contained. Either way the transforms and
load stay the same. Each aggregate is a small pure function of the raw tables
(see TRANSFORMS), which keeps them individually testable.
"""

import logging
import os
import sqlite3
from collections.abc import Callable
from contextlib import closing

import pandas as pd
from dotenv import load_dotenv

from config import REPORT_DB, SOURCE_DB

log = logging.getLogger(__name__)

RawTables = dict[str, pd.DataFrame]

SOURCE_TABLES = {
    "users": "users",
    "meetings": "meetings",
    "rsvps": "meeting_participants",
    "messages": "messages",
}
TIMESTAMP_COLUMNS = ("created_at", "scheduled_at", "sent_at")
TOP_HOSTS_LIMIT = 10
ROLLING_WINDOW_DAYS = 7


# ── Extract ───────────────────────────────────────────────────────────────────


def extract(source: sqlite3.Connection) -> RawTables:
    raw: RawTables = {}
    for name, table in SOURCE_TABLES.items():
        df = pd.read_sql(f"SELECT * FROM {table}", source)
        for col in TIMESTAMP_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        raw[name] = df
    return raw


def _parse_minutes(series: pd.Series) -> pd.Series:
    """Meeting duration is stored as free text ('30 min'); pull the leading number."""
    return pd.to_numeric(series.astype(str).str.extract(r"(\d+)")[0], errors="coerce")


def extract_supabase(conn) -> RawTables:
    """Extract from the live Lysbro Supabase database, normalised into the same shape
    the transforms expect. The real schema differs from the synthetic one (profiles
    instead of users, invite messages instead of an rsvp table, a lobby instead of a
    participant count), so this maps it across. User names are replaced with anonymous
    labels here, so no personal data ever reaches the reporting database or the charts."""
    # Cast uuid columns to text so they load into SQLite cleanly.
    users = pd.read_sql(
        "SELECT id::text AS id, plan, created_at FROM profiles ORDER BY created_at", conn
    )
    users["name"] = [f"Bruger {i}" for i in range(1, len(users) + 1)]

    meetings = pd.read_sql(
        """
        SELECT m.id::text      AS id,
               m.user_id::text AS host_id,
               m.created_at     AS scheduled_at,
               m.duration,
               (SELECT count(*) FROM meeting_lobby l
                 WHERE l.meeting_id = m.id::text AND l.status = 'admitted') AS participant_count
        FROM meetings m
        """,
        conn,
    )
    meetings["duration_min"] = _parse_minutes(meetings["duration"])
    meetings = meetings.drop(columns=["duration"])

    # RSVP lives on meeting-invite messages (invite_status), not a participant table.
    rsvps = pd.read_sql(
        "SELECT id::text AS meeting_id, invite_status AS rsvp "
        "FROM messages WHERE type = 'meeting_invite'",
        conn,
    )

    messages = pd.read_sql("SELECT id::text AS id, created_at AS sent_at FROM messages", conn)

    raw: RawTables = {"users": users, "meetings": meetings, "rsvps": rsvps, "messages": messages}
    for df in raw.values():
        for col in TIMESTAMP_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
    return raw


# ── Transform ─────────────────────────────────────────────────────────────────


def _month(series: pd.Series) -> pd.Series:
    return series.dt.to_period("M").astype(str)


def plan_distribution(raw: RawTables) -> pd.DataFrame:
    return raw["users"].groupby("plan").agg(user_count=("id", "count")).reset_index()


def monthly_signups(raw: RawTables) -> pd.DataFrame:
    users = raw["users"]
    return (
        users.assign(month=_month(users["created_at"]))
        .groupby("month")
        .agg(signups=("id", "count"))
        .reset_index()
    )


def monthly_meetings(raw: RawTables) -> pd.DataFrame:
    meetings = raw["meetings"]
    return (
        meetings.assign(month=_month(meetings["scheduled_at"]))
        .groupby("month")
        .agg(meeting_count=("id", "count"), avg_participants=("participant_count", "mean"))
        .reset_index()
        .assign(avg_participants=lambda df: df["avg_participants"].round(1))
    )


def top_hosts(raw: RawTables) -> pd.DataFrame:
    return (
        raw["meetings"]
        .groupby("host_id")
        .agg(meetings_hosted=("id", "count"), avg_duration_min=("duration_min", "mean"))
        .reset_index()
        .merge(raw["users"][["id", "name", "plan"]], left_on="host_id", right_on="id", how="left")
        .drop(columns=["id"])
        .sort_values(["meetings_hosted", "host_id"], ascending=[False, True])
        .head(TOP_HOSTS_LIMIT)
        .assign(avg_duration_min=lambda df: df["avg_duration_min"].round(0).fillna(0).astype(int))
    )


def rsvp_summary(raw: RawTables) -> pd.DataFrame:
    return (
        raw["rsvps"]
        .groupby("rsvp")
        .agg(rsvp_count=("meeting_id", "count"))
        .reset_index()
        .assign(pct=lambda df: (df["rsvp_count"] / df["rsvp_count"].sum() * 100).round(1))
    )


def daily_messages(raw: RawTables) -> pd.DataFrame:
    """Messages per calendar day, with gaps filled as 0 so the rolling average is honest."""
    messages = raw["messages"]
    daily = (
        messages.assign(date=messages["sent_at"].dt.normalize())
        .groupby("date")
        .agg(messages_sent=("id", "count"))
    )
    full_range = pd.date_range(daily.index.min(), daily.index.max(), freq="D")
    return (
        daily.reindex(full_range, fill_value=0)
        .rename_axis("date")
        .reset_index()
        .assign(
            rolling_7d=lambda df: (
                df["messages_sent"].rolling(ROLLING_WINDOW_DAYS, min_periods=1).mean().round(1)
            ),
            date=lambda df: df["date"].dt.strftime("%Y-%m-%d"),
        )
    )


def meetings_per_user_by_plan(raw: RawTables) -> pd.DataFrame:
    """Average meetings hosted per user, including users who hosted none."""
    hosted = raw["meetings"].groupby("host_id").agg(meeting_count=("id", "count"))
    return (
        raw["users"][["id", "plan"]]
        .merge(hosted, left_on="id", right_index=True, how="left")
        .fillna({"meeting_count": 0})
        .groupby("plan")
        .agg(avg_meetings=("meeting_count", "mean"), users=("id", "count"))
        .reset_index()
        .assign(avg_meetings=lambda df: df["avg_meetings"].round(1))
    )


TRANSFORMS: dict[str, Callable[[RawTables], pd.DataFrame]] = {
    "plan_distribution": plan_distribution,
    "monthly_signups": monthly_signups,
    "monthly_meetings": monthly_meetings,
    "top_hosts": top_hosts,
    "rsvp_summary": rsvp_summary,
    "daily_messages": daily_messages,
    "meetings_per_user_by_plan": meetings_per_user_by_plan,
}


def transform(raw: RawTables) -> dict[str, pd.DataFrame]:
    return {name: fn(raw) for name, fn in TRANSFORMS.items()}


# ── Load ──────────────────────────────────────────────────────────────────────


def load(results: dict[str, pd.DataFrame], target: sqlite3.Connection) -> None:
    for table_name, df in results.items():
        df.to_sql(table_name, target, if_exists="replace", index=False)
        log.info("loaded '%s' (%d rows)", table_name, len(df))


# ── Pipeline ──────────────────────────────────────────────────────────────────


def run() -> None:
    load_dotenv()
    database_url = os.environ.get("DATABASE_URL")

    log.info("Starting ETL pipeline")
    with closing(sqlite3.connect(REPORT_DB)) as target:
        if database_url:
            log.info("Extracting from Supabase (live production data)...")
            from sqlalchemy import create_engine

            engine = create_engine(database_url)
            try:
                with engine.connect() as source:
                    raw = extract_supabase(source)
            finally:
                engine.dispose()
        else:
            if not SOURCE_DB.exists():
                raise FileNotFoundError(
                    f"No DATABASE_URL set and no local source database at {SOURCE_DB}.\n"
                    "Set DATABASE_URL in .env to use live data, or run "
                    "`python generate_data.py` first."
                )
            log.info("Extracting from local synthetic database...")
            with closing(sqlite3.connect(SOURCE_DB)) as source:
                raw = extract(source)

        for name, df in raw.items():
            log.info("  %s: %d rows", name, len(df))

        log.info("Transforming...")
        results = transform(raw)

        log.info("Loading into reporting database...")
        load(results, target)

    log.info("Pipeline complete → %s", REPORT_DB)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    run()


if __name__ == "__main__":
    main()
