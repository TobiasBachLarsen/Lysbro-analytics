-- Lysbro analytics queries
-- Run against output/lysbro_analytics.db

-- Hvilken plan har den højeste gennemsnitlige mødeaktivitet?
SELECT plan, avg_meetings
FROM meetings_per_user_by_plan
ORDER BY avg_meetings DESC;

-- Hvilke måneder havde flest nye brugere?
SELECT month, signups
FROM monthly_signups
ORDER BY signups DESC
LIMIT 5;

-- Hvad er den samlede accept-rate på mødeindvitationer?
SELECT
    rsvp,
    count,
    pct || '%' AS pct
FROM rsvp_summary
ORDER BY count DESC;

-- Hvilken måned havde flest møder?
SELECT month, meeting_count, avg_participants
FROM monthly_meetings
ORDER BY meeting_count DESC
LIMIT 3;

-- Top 5 mest aktive brugere
SELECT name, plan, meetings_hosted, avg_duration_min
FROM top_hosts
LIMIT 5;
