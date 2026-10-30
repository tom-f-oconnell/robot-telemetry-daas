"""marts.sql splitting — no Spark needed."""

from __future__ import annotations

from rtdaas.spark.publish_marts import load_marts


def test_every_mart_is_a_select_with_its_partition_column():
    marts = {m.qualified: m for m in load_marts(date="2026-10-29")}
    assert marts["TELEMETRY.ROBOT_DAILY"].partition_column == "day"
    # no partition column in the header: the next line's SELECT must stay in the body
    assert marts["PROCEDURES.GESTURE_SKILL_SUMMARY"].partition_column is None
    for m in marts.values():
        assert m.sql.startswith("SELECT"), m.qualified
        assert "{" not in m.sql, m.qualified


def test_es_ids_do_not_depend_on_the_writer():
    """Stream and batch index the same events / KPI windows; equal rows must get equal ids."""
    import datetime as dt

    from rtdaas.spark.common import es_event_id, es_kpi_id

    ts = dt.datetime(2026, 10, 29, 12, 0, 0, 250000)
    stream = {"robot_id": "dv_001", "ts": ts, "code": "SESSION_START", "source": "stream"}
    bag = {**stream, "source": "bag", "recv_ts": ts, "bag_file": "x.mcap"}
    assert (
        es_event_id(stream) == es_event_id(bag) == "dv_001|2026-10-29T12:00:00.250000|SESSION_START"
    )
    window = {"robot_id": "dv_001", "arm": "PSM1", "window_start": ts.replace(microsecond=0)}
    assert es_kpi_id({**window, "computed_by": "stream"}) == es_kpi_id(
        {**window, "computed_by": "batch"}
    )
