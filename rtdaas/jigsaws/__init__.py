"""JIGSAWS dataset handling: layout discovery, kinematics/transcription parsing,
video frame features, and a synthetic generator that writes the same layout."""

from rtdaas.jigsaws.layout import (
    ARM_FIELDS,
    ARM_ORDER,
    TASKS,
    TrialMeta,
    discover_trials,
    label_frames,
    parse_kinematics,
    parse_meta_file,
    parse_transcription,
)

__all__ = [
    "ARM_FIELDS",
    "ARM_ORDER",
    "TASKS",
    "TrialMeta",
    "discover_trials",
    "label_frames",
    "parse_kinematics",
    "parse_meta_file",
    "parse_transcription",
]
