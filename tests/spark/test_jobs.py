"""Spark-level tests (run in CI's `spark` job with pyspark + Java installed)."""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("rtdaas-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_struct_types_build_for_every_table(spark):
    from rtdaas.spark import ddl

    for name in ddl.TABLES:
        st = ddl.struct_type(name)
        assert len(st.fields) == len(ddl.get(name).columns), name
        spark.createDataFrame([], st).count()


def test_kpi_aggregate_on_static_frame(spark):
    import datetime as dt

    from pyspark.sql import functions as F

    from rtdaas.spark.streaming_kpis import fold_robot_wide, kpi_aggregate, unify

    t0 = dt.datetime(2026, 10, 28, 10, 0, tzinfo=dt.UTC)
    pose = spark.createDataFrame(
        [
            ("dv_001", "PSM1", t0 + dt.timedelta(seconds=i), 0.01 if i % 2 else 0.0)
            for i in range(120)
        ],
        ["robot_id", "arm", "ts", "speed"],
    )
    events = spark.createDataFrame(
        [("dv_001", "", t0 + dt.timedelta(seconds=5), 2)], ["robot_id", "arm", "ts", "severity"]
    )
    heartbeat = spark.createDataFrame(
        [("dv_001", t0 + dt.timedelta(seconds=i)) for i in range(120)], ["robot_id", "ts"]
    )
    kpi = fold_robot_wide(kpi_aggregate(unify(pose, events, heartbeat)))
    out = kpi.where(F.col("arm") == "PSM1").orderBy("window_start").collect()
    assert len(out) == 2
    assert out[0]["n_samples"] == 60 and abs(out[0]["idle_fraction"] - 0.5) < 1e-6
    assert out[0]["warn_events"] == 1 and out[1]["warn_events"] == 0
    assert abs(out[0]["heartbeat_gap_s"] - 1.0) < 1e-6


def test_decode_bag_through_spark(spark, tmp_path, msg_registry):
    from pyspark.sql import functions as F

    from rtdaas.contracts.cdr import CdrCodec
    from rtdaas.sim.cli import main as sim_main
    from rtdaas.spark import ddl
    from rtdaas.spark.decode import decode_bag

    assert (
        sim_main(
            [
                "run",
                "--robots",
                "1",
                "--duration",
                "2",
                "--rate",
                "10",
                "--seed",
                "1",
                "--mcap-out",
                str(tmp_path),
                "--no-realtime",
            ]
        )
        == 0
    )
    rdd = spark.sparkContext.binaryFiles(str(tmp_path / "*" / "*" / "*" / "*" / "*.mcap"))
    codec = CdrCodec(msg_registry)
    rows = rdd.flatMap(
        lambda kv: decode_bag(bytes(kv[1]), codec, kv[0].rsplit("/", 1)[-1])["tool_pose"]
    ).collect()
    df = spark.createDataFrame(rows, schema=ddl.struct_type("telemetry.tool_pose_raw"))
    assert df.count() == 3 * 20 and df.select(F.countDistinct("arm")).first()[0] == 3
