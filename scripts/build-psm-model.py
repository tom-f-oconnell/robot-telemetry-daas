#!/usr/bin/env python3
"""Build the UI's da Vinci arm model from dVRK's meshes (jhu-dvrk/dvrk_model, MIT licence).

Writes ``rtdaas/serving/static/models/psm.glb``: one mesh per link of a Classic PSM with a Large
Needle Driver (400006), each already in its link's frame and decimated, because the originals run to
2 MB for a jaw a centimetre long. ``static/robot-sim.js`` holds the joints that connect the links.
The output is committed; re-run this only to change the model:

    pip install trimesh pycollada fast-simplification pillow scipy
    python scripts/build-psm-model.py
"""

from __future__ import annotations

import io
import urllib.request
from pathlib import Path

import numpy as np
import trimesh

MODEL_REF = "main"
RAW = f"https://raw.githubusercontent.com/jhu-dvrk/dvrk_model/{MODEL_REF}/meshes"
LFS = f"https://media.githubusercontent.com/media/jhu-dvrk/dvrk_model/{MODEL_REF}/meshes"
OUT = Path(__file__).resolve().parent.parent / "rtdaas/serving/static/models/psm.glb"
PI_2 = np.pi / 2
L_TOOL = 0.4162

# link: (base URL, mesh path, visual origin xyz, visual origin rpy, scale), from dvrk_model's
# urdf/Classic/PSM_base.urdf.xacro and urdf/common/instruments/*.
LINKS = {
    "base": (
        RAW,
        "arms/Classic/PSM/PSM_base.dae",
        (0.039, -0.40788, -0.07879),
        (1.5708, 0, 3.1416),
        1,
    ),
    "yaw": (RAW, "arms/Classic/PSM/PSM_yaw.dae", (0.0125, 0, 0.5265), (3.1416, 0, 1.5708), 1),
    "pitch_2": (RAW, "arms/Classic/PSM/PSM_pitch_2.dae", (0, 0, 0), (0, 0, -0.27129), 1),
    "pitch_3": (RAW, "arms/Classic/PSM/PSM_pitch_3.dae", (0, 0, 0), (0, 0, -0.27129), 1),
    "pitch_4": (RAW, "arms/Classic/PSM/PSM_pitch_4.dae", (0.009, 0, 0), (0, -1.5708, 0), 1),
    "pitch_5": (RAW, "arms/Classic/PSM/PSM_pitch_5.dae", (0.009, 0, 0), (0, -1.5708, 0), 1),
    "insertion": (
        RAW,
        "arms/Classic/PSM/PSM_insertion.dae",
        (0.02528, 0.429, 0),
        (0, -1.5708, 1.5708),
        1,
    ),
    "housing": (
        RAW,
        "instruments/housing/Classic/housing_Classic_body.dae",
        (0, 0, 0.0904),
        (0, 0, PI_2),
        1,
    ),
    "roll": (LFS, "instruments/roll/4670/roll_4670.obj", (0, 0, 0.02975), (-PI_2, 0, PI_2), 0.1),
    "wrist_pitch": (
        LFS,
        "instruments/wrist_pitch/0091/wrist_pitch_0091.obj",
        (0.0295, 0, 0),
        (0, PI_2, PI_2),
        0.1,
    ),
    "wrist_yaw": (
        LFS,
        "instruments/wrist_yaw/0091/wrist_yaw_0091.obj",
        (0, 0, -0.000236),
        (0, 0, 0),
        1,
    ),
    "jaw_1": (LFS, "instruments/tip/006/tip_006_1.obj", (0, 0, 0), (0, 0, 0), 1),
    "jaw_2": (LFS, "instruments/tip/006/tip_006_2.obj", (0, 0, 0), (0, 0, 0), 1),
}
MAX_FACES = 2500


def load_mesh(base: str, path: str) -> trimesh.Trimesh:
    with urllib.request.urlopen(f"{base}/{path}") as response:
        data = response.read()
    kind = path.rsplit(".", 1)[1]
    loaded = trimesh.load(io.BytesIO(data), file_type=kind, skip_materials=True, process=True)
    if isinstance(loaded, trimesh.Scene):
        loaded = loaded.to_geometry()
    return loaded


def origin_matrix(xyz, rpy, scale) -> np.ndarray:
    matrix = trimesh.transformations.euler_matrix(*rpy, axes="sxyz")  # URDF: Rz(y) Ry(p) Rx(r)
    matrix[:3, 3] = xyz
    matrix[:3, :3] *= scale
    return matrix


def main() -> None:
    scene = trimesh.Scene()
    for link, (base, path, xyz, rpy, scale) in LINKS.items():
        mesh = load_mesh(base, path)
        before = len(mesh.faces)
        if before > MAX_FACES:
            mesh = mesh.simplify_quadric_decimation(face_count=MAX_FACES)
        mesh.apply_transform(origin_matrix(xyz, rpy, scale))
        if link == "roll":
            # dvrk_model reuses a longer instrument's shaft here; cut it at this one's mount.
            mesh = mesh.slice_plane([0, 0, -L_TOOL], [0, 0, 1])
        mesh.visual = trimesh.visual.ColorVisuals(mesh)
        scene.add_geometry(mesh, node_name=link, geom_name=link)
        low, high = mesh.bounds
        print(
            f"{link:12s} {before:6d} -> {len(mesh.faces):5d} faces  {low.round(3)} .. {high.round(3)}"
        )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_bytes(scene.export(file_type="glb"))
    print(f"{OUT}: {OUT.stat().st_size / 1e6:.2f} MB")


if __name__ == "__main__":
    main()
