"""
ETL pipeline — extracts raw Lysbro data from SQLite, transforms it into
analytics-ready aggregates, and loads results into a separate reporting database.

Each aggregate is a small pure function of the raw tables (see TRANSFORMS), which
keeps them individually testable and makes adding a new report a one-line change.
"""

import logging
import sqlite3
from collections.abc import Callable
from contextlib import closing

import pandas as pd

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
        .assign(avg_duration_min=lambda df: df["avg_duration_min"].round(0).astype(int))
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
    if not SOURCE_DB.exists():
        raise FileNotFoundError(
            f"Source database not found: {SOURCE_DB}\nRun `python generate_data.py` first."
        )

    log.info("Starting ETL pipeline")
    with (
        closing(sqlite3.connect(SOURCE_DB)) as source,
        closing(sqlite3.connect(REPORT_DB)) as target,
    ):
        log.info("Extracting...")
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
