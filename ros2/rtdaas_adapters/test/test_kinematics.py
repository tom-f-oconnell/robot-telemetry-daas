"""Tests for the ROS-free helpers in rtdaas_adapters.kinematics (plain pytest, no rclpy)."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

# Make the package importable without an installed ROS workspace.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rtdaas_adapters.kinematics import (  # noqa: E402
    DEFAULT_JOINT_LIMITS,
    STATE_FAULT,
    STATE_HOMING,
    STATE_IDLE,
    STATE_READY,
    STATE_TELEOP,
    SpeedEstimator,
    StateTracker,
    arm_has_jaw,
    jaw_angle_from_joint_state,
    joint_limit_violations,
    operating_state_to_heartbeat_state,
)


def test_speed_estimator_constant_velocity_converges():
    est = SpeedEstimator(alpha=1.0)  # no filtering: raw finite difference
    assert est.update(0.0, 0.0, 0.0, 0.0) == 0.0
    # 0.02 m every 0.02 s along x -> 1 m/s
    for i in range(1, 6):
        v = est.update(i * 0.02, i * 0.02, 0.0, 0.0)
    assert v == pytest.approx(1.0, rel=1e-9)


def test_speed_estimator_filters_and_ignores_duplicates():
    est = SpeedEstimator(alpha=0.5)
    est.update(0.0, 0.0, 0.0, 0.0)
    v1 = est.update(0.1, 0.1, 0.0, 0.0)  # raw 1 m/s, filtered 0.5
    assert v1 == pytest.approx(0.5)
    v_dup = est.update(0.1, 0.1, 0.0, 0.0)  # non-increasing time -> unchanged
    assert v_dup == pytest.approx(0.5)
    v_gap = est.update(5.0, 10.0, 0.0, 0.0)  # gap > max_dt resets instead of spiking
    assert v_gap == 0.0


def test_jaw_angle_lookup_prefers_named_jaw_joint():
    assert jaw_angle_from_joint_state(["jaw"], [0.42]) == pytest.approx(0.42)
    assert jaw_angle_from_joint_state(["outer_jaw", "x"], [0.1, 0.9]) == pytest.approx(0.1)
    assert jaw_angle_from_joint_state([], [0.3]) == pytest.approx(0.3)
    assert math.isnan(jaw_angle_from_joint_state([], []))


def test_arm_has_jaw():
    assert arm_has_jaw("PSM1") and arm_has_jaw("psm2")
    assert not arm_has_jaw("ECM") and not arm_has_jaw("MTML")


def test_joint_limit_violations_with_margin():
    names = list(DEFAULT_JOINT_LIMITS)
    nominal = [0.0, 0.0, 0.1, 0.0, 0.0, 0.0]
    assert joint_limit_violations(names, nominal) == []
    # insertion at 0.235 is inside [0, 0.24] but within the 5 % margin of the top
    near_top = [0.0, 0.0, 0.235, 0.0, 0.0, 0.0]
    hits = joint_limit_violations(names, near_top)
    assert [h[0] for h in hits] == ["outer_insertion"]
    assert hits[0][2] == (0.0, 0.24)
    # unknown joints are ignored; short position lists are tolerated
    assert joint_limit_violations(["mystery", "outer_yaw"], [99.0]) == []


@pytest.mark.parametrize(
    "state,homed,busy,teleop,expected",
    [
        (None, None, None, False, STATE_IDLE),
        ("DISABLED", None, None, False, STATE_IDLE),
        ("PAUSED", True, False, False, STATE_IDLE),
        ("FAULT", True, False, True, STATE_FAULT),
        ("ENABLED", False, True, False, STATE_HOMING),
        ("ENABLED", None, True, False, STATE_HOMING),
        ("ENABLED", True, False, False, STATE_READY),
        ("ENABLED", True, False, True, STATE_TELEOP),
        ("enabled", True, False, True, STATE_TELEOP),
    ],
)
def test_operating_state_mapping(state, homed, busy, teleop, expected):
    assert operating_state_to_heartbeat_state(state, homed, busy, teleop) == expected


def test_state_tracker_emits_transition_and_estop_events():
    tr = StateTracker()
    first = tr.update("PSM1", STATE_READY)
    assert [c for _, c, _ in first] == ["STATE_READY"]
    assert tr.update("PSM1", STATE_READY) == []  # no change, no event
    fault = tr.update("PSM1", STATE_FAULT)
    assert [c for _, c, _ in fault] == ["STATE_FAULT", "ESTOP"]
    assert fault[1][0] == 3  # ERROR severity
    assert tr.robot_state() == STATE_FAULT
    cleared = tr.update("PSM1", STATE_READY)
    assert [c for _, c, _ in cleared] == ["STATE_READY", "FAULT_CLEARED"]
    tr.update("PSM2", STATE_TELEOP)
    tr.update("ECM", STATE_IDLE)
    assert tr.robot_state() == STATE_TELEOP
    assert tr.active_arms() == ["PSM1", "PSM2"]
