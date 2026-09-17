"""
ETL pipeline — extracts raw Lysbro data, transforms it into analytics-ready
aggregates, and loads results into a separate reporting database.

The source is chosen explicitly: `--source supabase` reads the live Lysbro database
(DATABASE_URL, see extract_supabase), `--source synthetic` (the default) reads the local
SQLite database made by generate_data.py, so the pipeline runs self-contained. Which one
produced the reporting database is recorded in its `pipeline_meta` table. Either way the
transforms and load stay the same. Each aggregate is a small pure function of the raw tables
(see TRANSFORMS), which keeps them individually testable.
"""

import argparse
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
# Lysbro's users are Danish; days and months are counted in their local time, not UTC.
LOCAL_TZ = "Europe/Copenhagen"


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
    # plan has a database default of 'gratis'; treat a NULL the same way rather than
    # letting pandas silently drop those users from every per-plan grouping.
    users = pd.read_sql(
        "SELECT id::text AS id, COALESCE(plan, 'gratis') AS plan, created_at "
        "FROM profiles ORDER BY created_at",
        conn,
    )
    # Anonymise: the reporting database gets a running number instead of the profile
    # uuid, and "Bruger N" instead of the name, so nothing in it identifies a person.
    anon_id = {uuid: n for n, uuid in enumerate(users["id"], start=1)}
    users["id"] = users["id"].map(anon_id)
    users["name"] = [f"Bruger {n}" for n in users["id"]]

    # A meeting's time is its scheduled date and time (text columns "2026-04-25" and
    # "13:00", entered by the host in local time), not created_at, which is when it was
    # booked. Duration is free text ("60 min") and may be missing for quick-start meetings.
    meetings = pd.read_sql(
        """
        SELECT m.id::text      AS id,
               m.user_id::text AS host_id,
               (m.date || ' ' || COALESCE(NULLIF(m.time, ''), '00:00'))::timestamp AS scheduled_at,
               m.duration,
               (SELECT count(*) FROM meeting_lobby l
                 WHERE l.meeting_id = m.id::text AND l.status = 'admitted') AS participant_count
        FROM meetings m
        """,
        conn,
    )
    meetings["host_id"] = meetings["host_id"].map(anon_id)
    meetings["duration_min"] = _parse_minutes(meetings["duration"])
    meetings = meetings.drop(columns=["duration"])

    # RSVP lives on meeting-invite messages (invite_status), not a participant table.
    rsvps = pd.read_sql(
        "SELECT id::text AS meeting_id, invite_status AS rsvp "
        "FROM messages WHERE type = 'meeting_invite'",
        conn,
    )

    # Only chat messages count as messages; meeting and organisation invitations are
    # also rows in `messages` (type 'meeting_invite' / 'org_invite') but are not chat.
    messages = pd.read_sql(
        "SELECT id::text AS id, created_at AS sent_at FROM messages WHERE type = 'text'", conn
    )

    raw: RawTables = {"users": users, "meetings": meetings, "rsvps": rsvps, "messages": messages}
    # created_at / sent_at are timestamptz (UTC) and are shifted to Danish wall-clock time
    # before the day and month buckets are made; scheduled_at is already wall-clock.
    for df in raw.values():
        for col in ("created_at", "sent_at"):
            if col in df.columns:
                df[col] = _to_local_naive(df[col])
    meetings["scheduled_at"] = pd.to_datetime(meetings["scheduled_at"])
    return raw


def _to_local_naive(series: pd.Series) -> pd.Series:
    """UTC timestamps -> naive timestamps in Danish local time."""
    return pd.to_datetime(series, utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)


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
        # A host whose meetings all lack a duration has an unknown average, not 0.
        .assign(avg_duration_min=lambda df: df["avg_duration_min"].round(0).astype("Int64"))
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
    if messages.empty:
        return pd.DataFrame(columns=["date", "messages_sent", "rolling_7d"])
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


SOURCES = ("synthetic", "supabase")


def run(source_name: str = "synthetic") -> None:
    if source_name not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES}, got {source_name!r}")

    log.info("Starting ETL pipeline (source: %s)", source_name)
    with closing(sqlite3.connect(REPORT_DB)) as target:
        if source_name == "supabase":
            load_dotenv()
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                raise RuntimeError(
                    "--source supabase needs DATABASE_URL (Supabase connection string) in .env"
                )
            # SQLAlchemy only knows the postgresql:// scheme; Supabase hands out postgres://.
            database_url = database_url.replace("postgres://", "postgresql://", 1)
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
                    f"No local source database at {SOURCE_DB}. "
                    "Run `python generate_data.py` first, or use --source supabase."
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

        # Record where the numbers came from, so a chart can never be mistaken for the
        # other source.
        pd.DataFrame(
            {"source": [source_name], "extracted_at": [pd.Timestamp.now(tz="UTC").isoformat()]}
        ).to_sql("pipeline_meta", target, if_exists="replace", index=False)

    log.info("Pipeline complete → %s", REPORT_DB)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    parser = argparse.ArgumentParser(description="Build the Lysbro reporting database.")
    parser.add_argument(
        "--source",
        choices=SOURCES,
        default="synthetic",
        help="synthetic: local SQLite made by generate_data.py (default); "
        "supabase: the live Lysbro database via DATABASE_URL",
    )
    run(parser.parse_args().source)


if __name__ == "__main__":
    main()
