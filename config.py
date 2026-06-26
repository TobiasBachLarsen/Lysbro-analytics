from pathlib import Path

SOURCE_DB = Path("output/lysbro.db")
REPORT_DB = Path("output/lysbro_analytics.db")

RANDOM_SEED = 42
DATE_START  = "2025-01-01"
DATE_END    = "2026-06-01"

PLANS        = ["gratis", "pro", "erhverv"]
PLAN_WEIGHTS = [0.70, 0.22, 0.08]

PLAN_MEETING_RANGE     = {"gratis": (1, 4),  "pro": (5, 20),  "erhverv": (15, 50)}
PLAN_PARTICIPANT_RANGE = {"gratis": (1, 2),  "pro": (2, 10),  "erhverv": (5, 30)}

MEETING_TITLES = [
    "Ugentligt standup", "Projektmøde", "1:1 med teamleder", "Sprint planning",
    "Kundemøde", "Demo", "Retrospektiv", "Onboarding", "Strategimøde", "Statusmøde",
]

MEETING_DURATIONS = [15, 30, 45, 60, 90]
