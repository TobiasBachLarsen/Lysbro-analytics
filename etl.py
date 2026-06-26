"""
ETL pipeline — extracts raw Lysbro data from SQLite, transforms it into
analytics-ready aggregates, and loads results into a separate reporting database.
"""

import logging
import sqlite3
from datetime import datetime

import pandas as pd

from config import REPORT_DB, SOURCE_DB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


# ── Extract ───────────────────────────────────────────────────────────────────

def extract(source: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    tables = {
        "users":    "SELECT * FROM users",
        "meetings": "SELECT * FROM meetings",
        "rsvps":    "SELECT * FROM meeting_participants",
        "messages": "SELECT * FROM messages",
    }
    raw: dict[str, pd.DataFrame] = {}
    for name, query in tables.items():
        df = pd.read_sql(query, source)
        # Parse ISO-8601 timestamp columns after loading (parse_dates is deprecated)
        for col in ("created_at", "scheduled_at", "sent_at"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        raw[name] = df
    return raw


# ── Transform ─────────────────────────────────────────────────────────────────

def transform(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    users    = raw["users"]
    meetings = raw["meetings"]
    rsvps    = raw["rsvps"]
    messages = raw["messages"]

    plan_distribution = (
        users.groupby("plan")
        .agg(user_count=("id", "count"))
        .reset_index()
    )

    monthly_signups = (
        users
        .assign(month=users["created_at"].dt.to_period("M"))
        .groupby("month")
        .agg(signups=("id", "count"))
        .reset_index()
        .assign(month=lambda df: df["month"].astype(str))
    )

    monthly_meetings = (
        meetings
        .assign(month=meetings["scheduled_at"].dt.to_period("M"))
        .groupby("month")
        .agg(
            meeting_count=("id", "count"),
            avg_participants=("participant_count", "mean"),
        )
        .reset_index()
        .assign(
            month=lambda df: df["month"].astype(str),
            avg_participants=lambda df: df["avg_participants"].round(1),
        )
    )

    top_hosts = (
        meetings
        .groupby("host_id")
        .agg(meetings_hosted=("id", "count"), avg_duration_min=("duration_min", "mean"))
        .reset_index()
        .merge(users[["id", "name", "plan"]], left_on="host_id", right_on="id", how="left")
        .drop(columns=["id"])
        .sort_values("meetings_hosted", ascending=False)
        .head(10)
        .assign(avg_duration_min=lambda df: df["avg_duration_min"].round(0).astype(int))
    )

    rsvp_summary = (
        rsvps
        .groupby("rsvp")
        .agg(count=("meeting_id", "count"))
        .reset_index()
        .assign(pct=lambda df: (df["count"] / df["count"].sum() * 100).round(1))
    )

    daily_messages = (
        messages
        .assign(date=messages["sent_at"].dt.normalize())
        .groupby("date")
        .agg(messages_sent=("id", "count"))
        .reset_index()
        .assign(
            rolling_7d=lambda df: df["messages_sent"].rolling(7, min_periods=1).mean().round(1),
        )
    )

    meetings_per_user_by_plan = (
        meetings
        .groupby("host_id")
        .agg(meeting_count=("id", "count"))
        .reset_index()
        .merge(users[["id", "plan"]], left_on="host_id", right_on="id", how="left")
        .groupby("plan")
        .agg(avg_meetings=("meeting_count", "mean"))
        .reset_index()
        .assign(avg_meetings=lambda df: df["avg_meetings"].round(1))
    )

    return {
        "plan_distribution":        plan_distribution,
        "monthly_signups":          monthly_signups,
        "monthly_meetings":         monthly_meetings,
        "top_hosts":                top_hosts,
        "rsvp_summary":             rsvp_summary,
        "daily_messages":           daily_messages,
        "meetings_per_user_by_plan": meetings_per_user_by_plan,
    }


# ── Load ──────────────────────────────────────────────────────────────────────

def load(results: dict[str, pd.DataFrame], target: sqlite3.Connection) -> None:
    for table_name, df in results.items():
        df.to_sql(table_name, target, if_exists="replace", index=False)
        logging.info("loaded '%s' (%d rows)", table_name, len(df))


# ── Pipeline ──────────────────────────────────────────────────────────────────

def run() -> None:
    if not SOURCE_DB.exists():
        raise FileNotFoundError(
            f"Source database not found: {SOURCE_DB}\n"
            "Run `python generate_data.py` first."
        )

    logging.info("Starting ETL pipeline")

    try:
        source = sqlite3.connect(SOURCE_DB)
        target = sqlite3.connect(REPORT_DB)

        logging.info("Extracting...")
        raw = extract(source)
        for name, df in raw.items():
            logging.info("  %s: %d rows", name, len(df))

        logging.info("Transforming...")
        results = transform(raw)

        logging.info("Loading into reporting database...")
        load(results, target)

    finally:
        source.close()
        target.close()

    logging.info("Pipeline complete → %s", REPORT_DB)


if __name__ == "__main__":
    run()
