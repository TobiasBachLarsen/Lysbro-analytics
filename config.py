"""Shared settings for data generation, the ETL pipeline and the notebook."""

from pathlib import Path

OUTPUT_DIR = Path("output")
SOURCE_DB = OUTPUT_DIR / "lysbro.db"
REPORT_DB = OUTPUT_DIR / "lysbro_analytics.db"

# ── Synthetic data generation ────────────────────────────────────────────────
RANDOM_SEED = 42
DATE_START = "2026-01-01"
DATE_END = "2026-09-01"

PLANS = ["gratis", "pro", "erhverv"]
PLAN_WEIGHTS = [0.70, 0.22, 0.08]

# Meetings hosted and participants per meeting, per plan (inclusive ranges).
PLAN_MEETING_RANGE = {"gratis": (1, 4), "pro": (5, 20), "erhverv": (15, 50)}
PLAN_PARTICIPANT_RANGE = {"gratis": (1, 2), "pro": (2, 10), "erhverv": (5, 30)}

RSVP_STATES = ["accepted", "pending", "declined"]
RSVP_WEIGHTS = [0.6, 0.3, 0.1]

MESSAGE_COUNT = 500

MEETING_TITLES = [
    "Ugentligt standup",
    "Projektmøde",
    "1:1 med teamleder",
    "Sprint planning",
    "Kundemøde",
    "Demo",
    "Retrospektiv",
    "Onboarding",
    "Strategimøde",
    "Statusmøde",
]

MEETING_DURATIONS = [15, 30, 45, 60, 90]
