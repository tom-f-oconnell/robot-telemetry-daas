# JIGSAWS: kinematics + video

The JIGSAWS dataset (JHU–Intuitive) contains synchronized da Vinci kinematics (76 variables at
30 Hz), stereo endoscope video and gesture annotations for suturing, knot-tying and needle-passing
trials. The platform treats it as a second batch source and joins video-derived features to the
kinematics at frame granularity.

![JIGSAWS flow](../diagrams/jigsaws-flow.svg)

## Dataset layout expected

```
<root>/
  Suturing/
    kinematics/AllGestures/Suturing_B001.txt      # 76 whitespace-separated floats per line, 30 Hz
    transcriptions/Suturing_B001.txt              # "<start_frame> <end_frame> G<n>" (1-based frames)
    video/Suturing_B001_capture1.avi              # 640×480 @ 30 fps (capture2 = right eye)
    meta_file_Suturing.txt                        # trial  skill(N/I/E)  GRS total  6 sub-scores
  Knot_Tying/ …   Needle_Passing/ …
```

Column order in the kinematics file: MTML (19), MTMR (19), PSML (19), PSMR (19); each 19 =
position (3), rotation matrix (9), linear velocity (3), angular velocity (3), gripper angle (1).

`rtdaas-jigsaws synth --out data/jigsaws` writes a synthetic dataset in exactly this layout
(including small rendered videos whose moving shapes track the synthetic PSM positions), so the
whole pipeline runs without the real data.

## Using the real dataset

Request it from JHU CIRL and unzip the task archives into one folder. Then tell the repository where
it is, in a `.env` that git ignores (see `.env.example`):

```bash
JIGSAWS_ROOT=/path/to/JIGSAWS
JIGSAWS_STAGE=/path/to/jigsaws-stage   # absolute; the browser-ready copy goes here
```

`make synth` and `make demo` then use it instead of the synthetic dataset. Both run `rtdaas-jigsaws
import`, which writes the copy the pipelines read, because the download needs three things first:

| The download has | The copy has |
|---|---|
| MPEG-4 Part 2 AVIs (DivX), which browsers cannot play | H.264 MP4s, frame for frame |
| A video per eye | The left eye (`capture1`) |
| 120 kinematics files: 16 with no video, transcript or skill score, and 1 empty | The 103 complete trials (39 suturing, 36 knot-tying, 28 needle-passing) |

Keep the data out of git and public images.

`make demo` replaces whatever dataset is already in the bucket and the `procedures` tables, because
synthetic trials reuse real ids and a re-ingest only upserts gestures: an earlier dataset's segments
would otherwise stay among the new one's. With real data it also deletes the local synthetic copy,
which the API would otherwise serve in place of the bucket's video. It transcodes only videos whose
source changed.

The Spark job and `make local-pipeline` read this copy. Pointed straight at the download, they would
ingest the incomplete trials, and the Spark job would pick an eye per trial at random.

### What the real data looks like

- 284,310 kinematics rows, 284,644 video frames and 1,703 gesture segments; 96 % of rows are labelled.
- Video and kinematics never have the same length. Across the 103 trials the video is 2 to 17 frames
  longer (median 5), except `Knot_Tying_F005`, whose source video is 470 frames shorter. No gesture
  segment runs past the end of either.
- The two eyes differ too (`Suturing_B001`: 5,640 and 5,637 frames, against 5,635 kinematics rows).
- Frame features come from the MP4s. On 1,500 frames of `Suturing_B001`, against the AVI: motion
  energy r = 0.996 (mean 5 % higher), sharpness r = 0.997 (mean 7 % higher).
- `gripper_cycles` is not yet meaningful: gripper angles run from -1.32 to 0.54 rad, so its threshold
  of 0.5 (`GRIPPER_OPEN`, chosen for the synthetic 0 to 1 range) only sees the jaw fully open, and
  62 % of segments score 0.

## Processing (`batch_jigsaws.py`)

1. `discover_trials(root)` on the driver → one `TrialMeta` per trial (skill level from the meta file).
2. `parallelize(trials)`; per trial on an executor:
    - `parse_kinematics` → 76 columns renamed to `<arm>_<field>`;
    - `parse_transcription` + `label_frames` → gesture label per frame;
    - `frame_features` (OpenCV) → per-frame mean intensity, motion energy, Laplacian sharpness;
    - `gesture_metrics` → per-segment path length, speed, gripper cycles, mean motion energy.
3. Write `procedures.trials`, `procedures.kinematics` (arm structs), `procedures.gestures`,
   `procedures.frame_features`; index gestures (with trial metadata) into `rtdaas-gestures`.
4. `quality_gate --suite procedures.yml` then publish `PROCEDURES.GESTURE_SKILL_SUMMARY`.

## Why frame features

Video and kinematics are recorded by different subsystems. Motion energy from the video should
track the kinematic speed of the instruments; a trial where they disagree (e.g. a frame offset or
a dropped segment) shows up as low correlation in the trial page and can be flagged with a `sql`
check. It is also the hook for richer video features later (tool detection, phase recognition)
without changing the table design.

## In the UI

The trial page plays the video with a kinematics scrubber: the current frame drives a cursor on
the PSM position/speed/gripper traces, the gesture ribbon shows which `G<n>` segment the frame is
in, and the frame-feature strip shows motion energy and sharpness. Gesture search filters
`procedures.gestures` by task, gesture code and skill level and jumps straight to the segment.

Beside the video, a 3-D [da Vinci simulation](../serving/api.md#da-vinci-simulation) replays the trial
in step with it.
