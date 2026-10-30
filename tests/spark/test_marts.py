"""marts.sql splitting — no Spark needed."""

from __future__ import annotations

from rtdaas.spark.publish_snowflake import load_marts


def test_every_mart_is_a_select_with_its_partition_column():
    marts = {m.qualified: m for m in load_marts(date="2026-10-29")}
    assert marts["TELEMETRY.ROBOT_DAILY"].partition_column == "day"
    # no partition column in the header: the next line's SELECT must stay in the body
    assert marts["PROCEDURES.GESTURE_SKILL_SUMMARY"].partition_column is None
    for m in marts.values():
        assert m.sql.startswith("SELECT"), m.qualified
        assert "{" not in m.sql, m.qualified
