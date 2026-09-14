# Lysbro Analytics Pipeline

An ETL data pipeline that extracts, transforms and loads usage data from the [Lysbro](https://lysbro.com) video conferencing platform into an analytics-ready reporting database, with visualisations in a Jupyter Notebook.

---

## What it does

```
generate_data.py  →  output/lysbro.db            (synthetic raw data, mirrors the production schema)
etl.py            →  output/lysbro_analytics.db  (7 reporting tables)
analysis.ipynb    →  output/*.png                (charts and insights)
queries.sql       →  example business questions answered against the reporting db
```

**Pipeline steps:**

1. **Extract** — reads the raw tables (users, meetings, participants, messages) from SQLite and parses timestamps
2. **Transform** — each reporting table is a small pure function of the raw tables, registered in `TRANSFORMS` in `etl.py`
3. **Load** — writes the results into a separate reporting database

---

## Analytics produced

| Table | Description |
|---|---|
| `plan_distribution` | User count per subscription tier |
| `monthly_signups` | New user registrations per month |
| `monthly_meetings` | Meeting volume and average participants per month |
| `top_hosts` | Ten most active users by meetings hosted |
| `rsvp_summary` | Meeting invitation accept / pending / decline split |
| `daily_messages` | Messages per day (gaps filled with 0) with a 7-day rolling average |
| `meetings_per_user_by_plan` | Average meetings hosted per user by plan, with sample size |

---

## Charts

| | |
|---|---|
| ![Plan distribution](output/plan_distribution.png) | ![Monthly signups](output/monthly_signups.png) |
| ![Monthly meetings](output/monthly_meetings.png) | ![RSVP breakdown](output/rsvp_breakdown.png) |
| ![Daily messages](output/daily_messages.png) | ![Meetings per user](output/meetings_per_user.png) |

---

## Tech stack

- **Python 3.12**
- **pandas** — transformation and aggregation
- **SQLite** — source and reporting database
- **matplotlib** + **Jupyter** — visualisation
- **pytest** + **ruff** — tests, linting and formatting

---

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt   # or requirements.txt for runtime only
```

**Run the full pipeline:**

```bash
python generate_data.py                       # generate synthetic source data
python etl.py                                 # run ETL
jupyter nbconvert --execute --inplace analysis.ipynb   # or open it interactively
```

**Check the code:**

```bash
pytest
ruff check . && ruff format --check .
```

---

## Note on data

`generate_data.py` creates deterministic synthetic data (28 users, roughly 190 meetings, 500 messages) that mirrors the Lysbro production schema. This keeps the repo self-contained and avoids exposing real user data.

The generator guarantees a few invariants that the tests enforce: no meeting is scheduled before its host signed up, nobody is invited to a meeting scheduled before they signed up, and `participant_count` always matches the participant rows.

In production the extract step would read from the Supabase PostgreSQL database instead of `output/lysbro.db`; the rest of the pipeline is unchanged.
