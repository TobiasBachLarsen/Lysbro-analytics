import pandas as pd
import pytest

import etl


@pytest.fixture
def raw() -> etl.RawTables:
    ts = pd.Timestamp
    users = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "plan": ["pro", "gratis", "gratis"],
            "created_at": [ts("2025-01-05"), ts("2025-01-20"), ts("2025-02-02")],
        }
    )
    meetings = pd.DataFrame(
        {
            "id": [10, 11, 12],
            "host_id": [1, 1, 2],
            "scheduled_at": [ts("2025-01-10"), ts("2025-02-10"), ts("2025-02-11")],
            "duration_min": [30, 60, 45],
            "participant_count": [2, 1, 0],
        }
    )
    rsvps = pd.DataFrame(
        {
            "meeting_id": [10, 10, 11],
            "user_id": [2, 3, 2],
            "rsvp": ["accepted", "declined", "accepted"],
        }
    )
    messages = pd.DataFrame(
        {
            "id": [100, 101, 102],
            "sender_id": [1, 2, 1],
            "receiver_id": [2, 1, 3],
            "sent_at": [ts("2025-03-01 09:00"), ts("2025-03-01 17:00"), ts("2025-03-04 12:00")],
        }
    )
    return {"users": users, "meetings": meetings, "rsvps": rsvps, "messages": messages}


def test_plan_distribution_counts_users_per_plan(raw):
    df = etl.plan_distribution(raw).set_index("plan")
    assert df.loc["gratis", "user_count"] == 2
    assert df.loc["pro", "user_count"] == 1


def test_monthly_signups_groups_by_calendar_month(raw):
    df = etl.monthly_signups(raw).set_index("month")
    assert df.loc["2025-01", "signups"] == 2
    assert df.loc["2025-02", "signups"] == 1


def test_top_hosts_is_sorted_and_joined_with_user_info(raw):
    df = etl.top_hosts(raw)
    assert list(df["host_id"]) == [1, 2]
    assert df.iloc[0]["name"] == "A"
    assert df.iloc[0]["meetings_hosted"] == 2
    assert df.iloc[0]["avg_duration_min"] == 45


def test_rsvp_summary_percentages_sum_to_100(raw):
    df = etl.rsvp_summary(raw)
    assert df["pct"].sum() == pytest.approx(100)
    assert df.set_index("rsvp").loc["accepted", "rsvp_count"] == 2


def test_daily_messages_fills_missing_days_with_zero(raw):
    df = etl.daily_messages(raw).set_index("date")
    assert list(df.index) == ["2025-03-01", "2025-03-02", "2025-03-03", "2025-03-04"]
    assert list(df["messages_sent"]) == [2, 0, 0, 1]
    assert df.loc["2025-03-04", "rolling_7d"] == pytest.approx(0.8)  # mean of [2,0,0,1], rounded


def test_meetings_per_user_by_plan_includes_users_without_meetings(raw):
    df = etl.meetings_per_user_by_plan(raw).set_index("plan")
    # user 2 hosted 1 meeting, user 3 hosted 0 → gratis average is 0.5
    assert df.loc["gratis", "avg_meetings"] == pytest.approx(0.5)
    assert df.loc["gratis", "users"] == 2
    assert df.loc["pro", "avg_meetings"] == pytest.approx(2)


def test_transform_produces_every_registered_table(raw):
    assert set(etl.transform(raw)) == set(etl.TRANSFORMS)
