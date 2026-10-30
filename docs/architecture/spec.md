# Engineering spec: interfaces, schemas, and conventions

This document is the contract between the components of `robot-telemetry-daas`.
Every module, job, DAG, manifest, and test is written against the names and
shapes defined here. If you change something here, change it everywhere.

Python package: `rtdaas` (Robot Telemetry Data-as-a-Service). Python >= 3.11.
Spark 3.5.x (Scala 2.12 artifacts), Iceberg 1.6.x, Kafka 3.8 (KRaft),
Airflow 2.10, Elasticsearch 8.x, Avro 1.11, ROS 2 Jazzy.

---

## 1. Repository layout

```
robot-telemetry-daas/
├── README.md
├── Makefile                      # dev/demo entry points (make demo, make test, make docs)
├── pyproject.toml                # package rtdaas; extras: spark, kafka, api, airflow, dev
├── mkdocs.yml
├── .github/workflows/            # ci.yml, contracts.yml, images.yml, deploy.yml, integration.yml
├── contracts/
│   ├── msgs/<pkg>/msg/<Type>.msg # ROS 2 message definitions = source of truth
│   ├── avro/<pkg>.<Type>.avsc    # generated message schemas (committed, CI-checked)
│   ├── avro/kafka/<topic>.avsc   # generated Kafka record schemas (envelope + msg)
│   └── registry.yml              # topic -> type mapping, compatibility mode, subject naming
├── rtdaas/
│   ├── __init__.py               # __version__
│   ├── config.py                 # Settings (env vars), see §9
│   ├── contracts/                # msgspec, avro_gen, avro_binary, wire, compat, registry, cdr, cli
│   ├── mcap_lite/                # minimal MCAP writer/reader (no external deps)
│   ├── sim/                      # ROS-less robot fleet simulator, Kafka producer, MCAP recorder, cli
│   ├── jigsaws/                  # JIGSAWS layout parser, synthetic generator, video frame features, cli
│   ├── metrics/                  # kinematics.py (sessions, KPI windows), gestures.py
│   ├── quality/                  # engine.py (rule engine), adapters.py (pandas, spark), cli
│   ├── local/                    # serial pure-Python pipeline -> sqlite (dev/test/demo without Spark)
│   ├── spark/                    # common.py, streaming_kpis.py, batch_mcap.py, batch_jigsaws.py,
│   │                             # quality_gate.py, publish_marts.py
│   ├── airflow/dags/             # telemetry_bag_ingest.py, telemetry_backfill.py, jigsaws_ingest.py,
│   │                             # dq_daily.py, contracts_sync.py
│   └── serving/                  # app.py (Starlette), backends/, es.py, airflow_client.py,
│                                 # openapi.yaml, static/ (UI)
├── ros2/                         # ROS 2 workspace packages
│   ├── rtdaas_msgs/              # ToolPose, SystemEvent, Heartbeat (same files as contracts/msgs/rtdaas_msgs)
│   ├── rtdaas_kafka_bridge/      # C++: generic ROS topic -> Avro -> Kafka
│   ├── rtdaas_adapters/          # Python: tool_pose_adapter, fleet_monitor
│   └── rtdaas_launch/            # launch files: dVRK sim fleet + adapters + bridge
├── infra/
│   ├── docker/                   # api.Dockerfile, spark.Dockerfile, airflow.Dockerfile, sim.Dockerfile, bridge.Dockerfile
│   ├── compose/                  # docker-compose.yml, .env.example, init/ (minio buckets, kafka topics, registry)
│   ├── terraform/                # modules/libvirt-vms, modules/aws-ec2, envs/local, envs/aws
│   ├── ansible/                  # inventory template, site.yml, roles/{common,k3s_server,k3s_agent,argocd}
│   └── k8s/                      # argocd/ (app-of-apps + Applications), helm-values/, manifests/rtdaas/
├── quality/checks/               # telemetry.yml, procedures.yml, streaming.yml
├── docs/                         # mkdocs-material site (architecture, contracts, pipelines, infra, runbooks, adr/)
├── scripts/                      # demo.sh, wait-for.sh, gen-fixtures.py
└── tests/                        # pytest; markers: spark, airflow, integration
```

Design rule: **all computation lives in pure-Python modules** (`contracts`,
`mcap_lite`, `sim`, `jigsaws`, `metrics`, `quality`, `local`). Spark jobs and
Airflow DAGs are thin orchestration around those functions so that the logic is
unit-tested without a cluster, and the same code produces identical results in
the local pipeline and on Spark.

---

## 2. Kafka topics and data contracts

### 2.1 Topics

| Kafka topic                     | ROS message type          | dVRK source topics (per arm `<A>`)          | Rate       |
|---------------------------------|---------------------------|---------------------------------------------|------------|
| `rtdaas.telemetry.joint_states` | `sensor_msgs/JointState`  | `/<robot>/<A>/measured_js`                  | 50–200 Hz  |
| `rtdaas.telemetry.tool_pose`    | `rtdaas_msgs/ToolPose`    | adapter from `/<robot>/<A>/measured_cp` + `/<robot>/<A>/jaw/measured_js` | 50 Hz |
| `rtdaas.telemetry.events`       | `rtdaas_msgs/SystemEvent` | fleet_monitor node                          | sporadic   |
| `rtdaas.telemetry.heartbeat`    | `rtdaas_msgs/Heartbeat`   | fleet_monitor node                          | 1 Hz       |

- Partitions: 12 for `joint_states` and `tool_pose`, 3 for `events` and `heartbeat`.
- Key: `robot_id` (UTF-8 string), so all data for one robot is ordered within a partition.
- Retention: 7 days (`joint_states`, `tool_pose`), 30 days (`events`, `heartbeat`).
- Kafka record headers (informational; the same fields are also inside the value envelope):
  `robot_id`, `ros_topic`, `msg_type`, `schema_id`.

Robot IDs: `dv_001`, `dv_002`, … (string). Arm names: `PSM1`, `PSM2`, `PSM3`, `ECM`, `MTML`, `MTMR`.

### 2.2 Value serialization

Confluent wire format: `0x00` magic byte, 4-byte big-endian schema id, then Avro
binary of the **Kafka record schema** (envelope). The registry is Apicurio
Registry in Confluent-compatible mode (`/apis/ccompat/v7`), so the same client
code works against Confluent Schema Registry.

Subject naming: `<kafka topic>-value`, e.g. `rtdaas.telemetry.tool_pose-value`.
Compatibility mode: `BACKWARD` on every subject (new schema must read old data).

### 2.3 ROS `.msg` → Avro mapping (`rtdaas.contracts.avro_gen`)

| ROS type                         | Avro type                         |
|----------------------------------|-----------------------------------|
| `bool`                           | `boolean`                         |
| `int8`, `int16`, `int32`, `uint8`, `uint16`, `byte`, `char` | `int`  |
| `int64`, `uint32`, `uint64`      | `long`                            |
| `float32`                        | `float`                           |
| `float64`                        | `double`                          |
| `string`, `wstring`              | `string`                          |
| `T[]`, `T[N]`, `T[<=N]`          | `{"type":"array","items": T}`     |
| `pkg/Type` or `Type` (same pkg)  | nested `record` (defined once, then referenced by full name) |

- Avro record name = message type name, namespace = ROS package (`sensor_msgs.JointState`).
- Constants (`uint8 SEVERITY_INFO=1`) are **not** fields; they are listed in the record `doc`.
- A field default in the `.msg` (`float64 w 1`) becomes the Avro `default`.
- Comments immediately above a field become the field `doc`.
- Field order is preserved.

**Kafka record schema** (envelope), generated by `kafka_record_schema(msg_schema, topic)`:

```json
{"type":"record","name":"ToolPoseRecord","namespace":"rtdaas.kafka",
 "fields":[
   {"name":"robot_id","type":"string"},
   {"name":"ros_topic","type":"string"},
   {"name":"recv_time_ns","type":"long","doc":"bridge receive time, UNIX ns"},
   {"name":"msg","type": <message record schema>}]}
```

Record name = `<MsgType>Record`. Files: `contracts/avro/<pkg>.<Type>.avsc` (message
schemas) and `contracts/avro/kafka/<topic>.avsc` (envelopes). CI fails if the
committed files differ from a fresh `rtdaas-contracts gen`.

### 2.4 Contract enforcement

- `rtdaas-contracts check --base <git-ref|dir> --head contracts/avro --mode BACKWARD`
  runs the pure-Python compatibility checker (`rtdaas.contracts.compat`) between the
  schema on `main` and the working tree. It is a required CI check.
- `rtdaas-contracts register --registry $SCHEMA_REGISTRY_URL` sets the subject
  compatibility mode and registers each envelope schema (idempotent). Run by the
  `contracts_sync` DAG and by the compose init container.
- Bridge, simulator and Spark jobs **never embed schemas**; they fetch the latest
  subject version from the registry at startup (`rtdaas.contracts.registry.RegistryClient`).

### 2.5 CDR (ROS 2 bag payloads)

`ros2 bag record` writes MCAP with `message_encoding = "cdr"` and
`schema.encoding = "ros2msg"`. `rtdaas.contracts.cdr` implements XCDR1
little-endian encode/decode driven by the parsed `.msg` definitions:

- 4-byte encapsulation header (`00 01 00 00` = CDR_LE), alignment origin = byte 4.
- Primitives aligned to their size (max 8). `string` = `uint32` length incl. NUL +
  bytes + NUL. `T[]`/`T[<=N]` = `uint32` count + elements. `T[N]` = N elements.
  Nested messages inline.
- Golden vector: `std_msgs/String{data:"hello"}` →
  `00 01 00 00 06 00 00 00 68 65 6c 6c 6f 00`.

`schema.data` for `ros2msg` is the message definition followed by its dependencies,
each separated by `================================================================================\nMSG: <pkg>/<Type>\n`.

---

## 3. Storage layer

### 3.1 Object storage (S3 / MinIO)

| Bucket        | Prefix                                            | Content                               |
|---------------|---------------------------------------------------|---------------------------------------|
| `rtdaas-raw`  | `bags/<robot_id>/<yyyy>/<mm>/<dd>/<session>.mcap` | recorded bags (landing zone)          |
| `rtdaas-raw`  | `jigsaws/<Task>/{kinematics,transcriptions,video}/` | JIGSAWS (or synthetic) dataset      |
| `rtdaas-lake` | `warehouse/`                                      | Iceberg warehouse                     |
| `rtdaas-lake` | `checkpoints/<job>/`                              | Spark streaming checkpoints           |

### 3.2 Iceberg tables (catalog `lake`, REST catalog)

Namespaces: `telemetry`, `procedures`, `dq`. Timestamps are `timestamp` (UTC,
microseconds). Partitioning in brackets. `source` ∈ {`stream`, `bag`}.

**telemetry.joint_states_raw** [days(ts), robot_id]
`robot_id string, arm string, ts timestamp, recv_ts timestamp, frame_id string, joint_names array<string>, position array<double>, velocity array<double>, effort array<double>, source string, bag_file string`

**telemetry.tool_pose_raw** [days(ts), robot_id]
`robot_id string, arm string, ts timestamp, recv_ts timestamp, x double, y double, z double, qx double, qy double, qz double, qw double, jaw_angle double, speed double, source string, bag_file string`

**telemetry.events_raw** [days(ts)]
`robot_id string, arm string, ts timestamp, recv_ts timestamp, severity int, severity_name string, code string, message string, source string, bag_file string`

**telemetry.heartbeat_raw** [days(ts)]
`robot_id string, ts timestamp, recv_ts timestamp, uptime_s double, state string, active_arms array<string>, software_version string, source string, bag_file string`

**telemetry.kpi_1m** [days(window_start)] — one row per robot × arm × 1-minute window
`robot_id string, arm string, window_start timestamp, window_end timestamp, n_samples long, mean_speed_mps double, max_speed_mps double, path_length_m double, idle_fraction double, jaw_cycles long, warn_events long, error_events long, heartbeat_gap_s double, computed_by string, computed_at timestamp`

**telemetry.sessions** [days(start_ts)] — "economy of motion" per session × arm
`session_id string, robot_id string, arm string, start_ts timestamp, end_ts timestamp, duration_s double, n_samples long, path_length_m double, mean_speed_mps double, idle_fraction double, jaw_cycles long, warn_events long, error_events long, instrument_swaps long, bag_file string, pipeline_version string, processed_at timestamp`

Session definition: bounded by `SESSION_START` / `SESSION_END` events for a robot;
if absent, a heartbeat gap > 60 s or the bag file boundary splits sessions.
`session_id = f"{robot_id}-{start_ts:%Y%m%dT%H%M%S}"`. Idle = speed < 0.005 m/s.
Jaw cycle = jaw_angle crossing 0.1 rad upward. Path length = sum of 3-D step distances.

**procedures.trials** [task]
`trial_id string, task string, subject string, repetition int, skill_level string, grs_total int, duration_s double, n_frames long, n_kinematic_samples long, sample_rate_hz double, video_path string, kinematics_path string, transcription_path string, n_gestures long, processed_at timestamp, pipeline_version string`

`trial_id` = JIGSAWS file stem, e.g. `Suturing_B001`. Task ∈ {`Suturing`, `Knot_Tying`, `Needle_Passing`}.
Subject = letter (B..I), repetition = 001..005. skill_level ∈ {`novice`, `intermediate`, `expert`}
from the meta file's self-reported experience (`N`/`I`/`E`).

**procedures.kinematics** [task, trial_id] — one row per kinematic sample (30 Hz)
`trial_id string, task string, frame_idx long, t_s double, mtml struct<...>, mtmr struct<...>, psml struct<...>, psmr struct<...>, gesture string`
where each arm struct = `x,y,z double, r11..r33 double (9), vx,vy,vz double, wx,wy,wz double, gripper double`.
Column order in the JIGSAWS 76-column file: MTML(19), MTMR(19), PSML(19), PSMR(19); each 19 = pos(3), rot(9), lin vel(3), ang vel(3), gripper(1).

**procedures.gestures** [task]
`trial_id string, task string, subject string, skill_level string, gesture string, start_frame long, end_frame long, duration_s double, path_length_psml double, path_length_psmr double, mean_speed_psml double, mean_speed_psmr double, gripper_cycles long, motion_energy_mean double, n_frames long`

**procedures.frame_features** [task, trial_id]
`trial_id string, task string, frame_idx long, t_s double, mean_intensity double, motion_energy double, sharpness double, gesture string`

**dq.check_results** [days(run_ts)]
`run_id string, suite string, table_name string, check_name string, severity string, status string, observed double, threshold string, partition_value string, run_ts timestamp, message string`
`status` ∈ {`pass`, `fail`, `error`, `skipped`}; `severity` ∈ {`blocking`, `warning`}.

### 3.3 SQLite mirror (local pipeline and API dev backend)

The local pipeline writes the same tables to SQLite with `.` → `__`:
`telemetry__tool_pose_raw`, `telemetry__events_raw`, `telemetry__heartbeat_raw`,
`telemetry__joint_states_raw`, `telemetry__kpi_1m`, `telemetry__sessions`,
`procedures__trials`, `procedures__kinematics`, `procedures__gestures`,
`procedures__frame_features`, `dq__check_results`, plus `catalog` (see §6) and
`jobs` (reprocess requests: `job_id, kind, target, requested_at, status, dag_run_id`).
Arrays are stored as JSON text; structs are flattened with `_` (`psml_x`, `psml_gripper`);
timestamps as ISO-8601 UTC strings.

### 3.4 Elasticsearch indices

| Index (alias)        | Document                                                 |
|----------------------|----------------------------------------------------------|
| `rtdaas-events`      | events_raw row + `@timestamp` = ts                        |
| `rtdaas-kpi-1m`      | kpi_1m row + `@timestamp` = window_start                  |
| `rtdaas-sessions`    | sessions row + `@timestamp` = start_ts                    |
| `rtdaas-gestures`    | gestures row + trial metadata (task, subject, skill_level)|
Document id for idempotent upserts: events `sha1(robot_id|ts|code)`, kpi `robot_id|arm|window_start`,
sessions `session_id|arm`, gestures `trial_id|start_frame`.

### 3.5 Curated marts (Iceberg `marts.*`)

Defined in `rtdaas/spark/marts.sql` as `SCHEMA.TABLE`, written by `publish_marts.py` to
`lake.marts.<table>` (ADR 0012; originally copied to Snowflake):
- `marts.robot_daily(robot_id, day, sessions, active_s, path_length_m, warn_events, error_events, instrument_swaps, mean_idle_fraction, processed_at)` ← `TELEMETRY.ROBOT_DAILY`
- `marts.session_daily` = sessions table + `day` ← `TELEMETRY.SESSION_DAILY`
- `marts.gesture_skill_summary(task, gesture, skill_level, n, mean_duration_s, mean_path_length_psml, mean_path_length_psmr, mean_speed_psml, mean_speed_psmr, mean_gripper_cycles, mean_motion_energy, day)` ← `PROCEDURES.GESTURE_SKILL_SUMMARY`
- `marts.check_results` = dq.check_results + `day` ← `DQ.CHECK_RESULTS`

Day-partitioned (`day`) except the gesture summary, which is replaced whole.

---

## 4. Python module interfaces

All functions below are pure Python (pandas/numpy allowed) unless noted.

### 4.1 `rtdaas.contracts`

```python
# msgspec.py
@dataclass
class Field:  name: str; type: str; is_array: bool; array_len: int|None; bounded: bool; default: Any|None; doc: str|None
@dataclass
class Constant: name: str; type: str; value: Any
@dataclass
class MsgDef: package: str; name: str; fields: list[Field]; constants: list[Constant]; raw: str
    @property
    def full_name(self) -> str  # "sensor_msgs/JointState"
class MsgRegistry:
    @classmethod
    def load_dir(cls, root: str|Path) -> "MsgRegistry"       # root = contracts/msgs
    def get(self, full_name: str) -> MsgDef                   # "sensor_msgs/JointState"
    def dependencies(self, full_name: str) -> list[str]       # transitive, deterministic order
    def ros2msg_schema_text(self, full_name: str) -> str      # MCAP ros2msg schema data
    def __iter__(self) -> Iterator[MsgDef]
PRIMITIVES: frozenset[str]
def parse_msg(package: str, name: str, text: str) -> MsgDef

# avro_gen.py
def msg_to_avro(msg: MsgDef, registry: MsgRegistry) -> dict
def kafka_record_schema(msg_schema: dict, topic: str) -> dict
def generate_all(registry: MsgRegistry, topics: dict[str, str], out_dir: Path) -> list[Path]
    # topics: kafka topic -> msg full name (from contracts/registry.yml)

# avro_binary.py   (pure Python Avro binary codec; fastavro used if importable)
def encode(schema: dict, datum: Any) -> bytes
def decode(schema: dict, data: bytes) -> Any
def parse_schema(schema: dict) -> dict   # resolves named-type references, validates

# wire.py
MAGIC = b"\x00"
def encode_wire(schema_id: int, schema: dict, datum: Any) -> bytes
def decode_wire(data: bytes, schema_for_id: Callable[[int], dict]) -> tuple[int, Any]

# compat.py
@dataclass
class Incompatibility: path: str; kind: str; detail: str
def check_backward(reader: dict, writer: dict) -> list[Incompatibility]  # reader = new, writer = old
def check_forward(reader: dict, writer: dict) -> list[Incompatibility]
def check(new: dict, old: dict, mode: str) -> list[Incompatibility]      # BACKWARD | FORWARD | FULL | NONE

# registry.py  (Confluent-compatible REST; Apicurio ccompat)
class RegistryClient:
    def __init__(self, base_url: str, timeout: float = 10.0)
    def set_compatibility(self, subject: str, mode: str) -> None
    def register(self, subject: str, schema: dict) -> int                  # returns schema id
    def latest(self, subject: str) -> tuple[int, int, dict]                # (id, version, schema)
    def by_id(self, schema_id: int) -> dict
    def test_compatibility(self, subject: str, schema: dict) -> bool
def subject_for_topic(topic: str) -> str   # f"{topic}-value"

# cdr.py
class CdrCodec:
    def __init__(self, registry: MsgRegistry)
    def encode(self, type_name: str, msg: dict) -> bytes      # includes encapsulation header
    def decode(self, type_name: str, data: bytes) -> dict

# cli.py  -> console script `rtdaas-contracts`
#   gen [--msgs DIR] [--registry-yml FILE] [--out DIR]
#   check --base REF_OR_DIR [--head DIR] [--mode BACKWARD]      exit 1 on incompatibility
#   register --registry URL [--avro DIR]
#   diff --base REF_OR_DIR [--head DIR]                        human-readable field diff
```

Message dicts: nested dicts keyed by field name; arrays as lists; `Time` as `{"sec": int, "nanosec": int}`.

### 4.2 `rtdaas.mcap_lite`

```python
class McapWriter:
    def __init__(self, path: str|Path, profile: str = "ros2", library: str = "rtdaas.mcap_lite")
    def add_schema(self, name: str, encoding: str, data: bytes) -> int
    def add_channel(self, topic: str, message_encoding: str, schema_id: int, metadata: dict[str,str]|None=None) -> int
    def write_message(self, channel_id: int, log_time_ns: int, publish_time_ns: int, data: bytes, sequence: int|None=None) -> None
    def close(self) -> None            # writes DataEnd, Summary (schemas, channels, statistics), Footer
    # context manager
@dataclass
class McapSchema: id: int; name: str; encoding: str; data: bytes
@dataclass
class McapChannel: id: int; topic: str; message_encoding: str; schema_id: int; metadata: dict
@dataclass
class McapMessage: channel_id: int; sequence: int; log_time_ns: int; publish_time_ns: int; data: bytes
def read_messages(path_or_bytes) -> Iterator[tuple[McapSchema|None, McapChannel, McapMessage]]
def read_summary(path_or_bytes) -> dict   # {"schemas":..., "channels":..., "message_count":..., "start_ns":..., "end_ns":...}
```
Unchunked, uncompressed files; CRC fields written as 0 (allowed by the spec).
Output must be readable by the official `mcap` Python library and `mcap info`.

### 4.3 `rtdaas.sim`

```python
# robot.py
@dataclass
class SimConfig: robot_id: str; arms: tuple[str,...] = ("PSM1","PSM2","ECM"); rate_hz: float = 50.0; seed: int = 0; software_version: str = "sim-1.0"
class RobotSim:
    def __init__(self, cfg: SimConfig)
    def tick(self, t_ns: int) -> list[tuple[str, str, dict]]   # (kafka_topic, ros_topic, msg_dict) for this tick
    # Produces: JointState per arm (6 joints PSM/ECM: outer_yaw, outer_pitch, outer_insertion, outer_roll, outer_wrist_pitch, outer_wrist_yaw),
    # ToolPose per arm (smooth pseudo-surgical motion with idle periods), Heartbeat at 1 Hz, SystemEvents:
    # SESSION_START at t0, SESSION_END at end, random INSTRUMENT_SWAP, CLUTCH_ENGAGED/RELEASED, JOINT_LIMIT_WARN (severity 2), ESTOP (severity 3, rare)
def ros_topic(robot_id: str, arm: str, suffix: str) -> str    # f"/{robot_id}/{arm}/{suffix}"

# recorder.py
class McapRecorder:                 # writes a ros2-bag-like MCAP (cdr + ros2msg) per robot session
    def __init__(self, out_path: Path, registry: MsgRegistry, topics: dict[str,str])
    def record(self, ros_topic: str, type_name: str, msg: dict, log_time_ns: int) -> None
    def close(self) -> None

# producer.py (requires confluent_kafka)
class KafkaProducer:
    def __init__(self, bootstrap: str, registry_url: str, topics: dict[str,str])   # fetches schema ids at init
    def send(self, kafka_topic: str, robot_id: str, ros_topic: str, msg: dict, recv_time_ns: int) -> None
    def flush(self) -> None

# cli.py -> console script `rtdaas-sim`
#   run --robots N --duration S [--rate HZ] [--seed S] [--kafka] [--mcap-out DIR] [--realtime/--no-realtime] [--start-time ISO]
#   MCAP files are written to <DIR>/<robot_id>/<yyyy>/<mm>/<dd>/<session_id>.mcap
```

### 4.4 `rtdaas.jigsaws`

```python
# layout.py
@dataclass
class TrialMeta: trial_id: str; task: str; subject: str; repetition: int; skill_level: str; grs_total: int|None; kinematics_path: Path; transcription_path: Path|None; video_path: Path|None
def discover_trials(root: Path, tasks: Iterable[str]|None=None) -> list[TrialMeta]
#   expects <root>/<Task>/kinematics/AllGestures/<Task>_<S><RRR>.txt, <root>/<Task>/transcriptions/<trial>.txt,
#   <root>/<Task>/video/<trial>_capture1.avi (or .mp4), <root>/<Task>/meta_file_<Task>.txt
ARM_ORDER = ("mtml", "mtmr", "psml", "psmr")
ARM_FIELDS = ("x","y","z","r11","r12","r13","r21","r22","r23","r31","r32","r33","vx","vy","vz","wx","wy","wz","gripper")
def parse_kinematics(path: Path, sample_rate_hz: float = 30.0) -> pd.DataFrame   # columns: frame_idx, t_s, <arm>_<field> × 76
def parse_transcription(path: Path) -> pd.DataFrame                              # columns: start_frame, end_frame, gesture
def parse_meta_file(path: Path) -> dict[str, tuple[str, int]]                     # trial_id -> (skill_level, grs_total)
def label_frames(kin: pd.DataFrame, transcript: pd.DataFrame) -> pd.DataFrame     # adds 'gesture' (None outside segments)

# video.py (OpenCV)
def frame_features(video_path: Path, max_frames: int|None=None) -> pd.DataFrame   # frame_idx, t_s, mean_intensity, motion_energy, sharpness
#   motion_energy = mean |gray[t] - gray[t-1]| / 255 ; sharpness = variance of Laplacian ; t_s = frame_idx / fps

# synth.py
def generate_dataset(out_dir: Path, n_trials_per_task: int = 2, tasks=("Suturing","Knot_Tying","Needle_Passing"), seed: int = 0, fps: float = 30.0, duration_s: float = 6.0, video: bool = True) -> list[TrialMeta]
#   writes JIGSAWS-layout files; video rendered with OpenCV (moving shapes driven by the synthetic PSM positions)

# cli.py -> console script `rtdaas-jigsaws`
#   synth --out DIR [--trials N] [--seed S] [--duration S] [--no-video]
#   inspect --root DIR
```

### 4.5 `rtdaas.metrics`

```python
# kinematics.py
IDLE_SPEED_MPS = 0.005; JAW_OPEN_RAD = 0.1
def speeds(df: pd.DataFrame) -> np.ndarray           # from x,y,z,ts columns; len == len(df); speeds[0]=0
def path_length(df: pd.DataFrame) -> float
def jaw_cycles(jaw_angle: pd.Series) -> int
def session_metrics(pose: pd.DataFrame, events: pd.DataFrame|None=None) -> dict
#   pose columns: ts (datetime64[ns, UTC]), x,y,z,jaw_angle ; one robot × arm × session
#   returns keys matching telemetry.sessions numeric columns (duration_s, n_samples, path_length_m, mean_speed_mps, idle_fraction, jaw_cycles, warn_events, error_events, instrument_swaps)
def kpi_windows(pose: pd.DataFrame, events: pd.DataFrame|None, heartbeat: pd.DataFrame|None, window: str = "1min") -> pd.DataFrame
#   returns telemetry.kpi_1m columns (without robot_id/arm/computed_*), one row per window
def split_sessions(events: pd.DataFrame, heartbeat: pd.DataFrame|None, span: tuple[pd.Timestamp,pd.Timestamp], gap_s: float = 60.0) -> list[tuple[pd.Timestamp,pd.Timestamp]]
def session_id(robot_id: str, start_ts: pd.Timestamp) -> str

# gestures.py
def gesture_metrics(kin: pd.DataFrame, transcript: pd.DataFrame, frames: pd.DataFrame|None, sample_rate_hz: float = 30.0) -> pd.DataFrame
#   returns procedures.gestures metric columns per transcript row
```

### 4.6 `rtdaas.quality`

Check suite YAML (`quality/checks/*.yml`):

```yaml
suite: telemetry
tables:
  - table: telemetry.tool_pose_raw
    partition_column: ts          # optional; CLI --partition applies a date filter on it
    checks:
      - name: not_null_ts          # built-ins below
        type: not_null
        column: ts
        severity: blocking
      - name: row_count_min
        type: row_count
        min: 1000
        severity: blocking
      - name: speed_range
        type: value_range
        column: speed
        min: 0
        max: 2.0
        max_violation_fraction: 0.001
        severity: warning
      - name: sample_rate
        type: sample_rate
        ts_column: ts
        group_by: [robot_id, arm]
        expected_hz: 50
        tolerance_fraction: 0.1
        severity: warning
      - name: schema
        type: schema_match
        expected: spec              # compares to the §3.2 schema registry in rtdaas.quality.schemas
        severity: blocking
      - name: unique_key
        type: unique
        columns: [robot_id, arm, ts]
        severity: blocking
      - name: freshness
        type: freshness
        ts_column: ts
        max_age_hours: 26
        severity: warning
      - name: no_estop_spike
        type: sql                 # arbitrary SQL returning a single numeric; compared with op/threshold
        query: "SELECT COUNT(*) FROM {table} WHERE code = 'ESTOP'"
        op: "<="
        threshold: 5
        severity: warning
```

```python
# engine.py
@dataclass
class CheckResult: suite: str; table_name: str; check_name: str; severity: str; status: str; observed: float|None; threshold: str; partition_value: str|None; message: str
def load_suite(path: Path) -> dict
def run_suite(suite: dict, adapter: "TableAdapter", partition_value: str|None=None, run_id: str|None=None) -> list[CheckResult]
def to_frame(results: list[CheckResult], run_id: str, run_ts) -> pd.DataFrame     # dq.check_results columns
def has_blocking_failures(results) -> bool

# adapters.py
class TableAdapter(Protocol):
    def frame(self, table_name: str, partition_column: str|None, partition_value: str|None) -> pd.DataFrame
    def sql(self, query: str) -> float|None      # query uses {table} placeholder already substituted
    def columns(self, table_name: str) -> dict[str,str]
class PandasAdapter(TableAdapter)   # dict[table_name, DataFrame]; sql via sqlite in-memory
class SqliteAdapter(TableAdapter)   # local mirror (§3.3)
class SparkAdapter(TableAdapter)    # Iceberg tables via spark.table(f"lake.{name}"); sql via spark.sql  (import pyspark lazily)
# schemas.py:  SPEC_SCHEMAS: dict[table_name, dict[column, type]]  (from §3.2)
# cli.py -> console script `rtdaas-quality`
#   run --suite FILE --backend sqlite --db PATH [--partition 2026-10-29] [--out-table]   exit 2 on blocking failures
```

### 4.7 `rtdaas.local`

```python
# pipeline.py
def run(bags_dir: Path|None, jigsaws_dir: Path|None, sqlite_path: Path, msgs_dir: Path = DEFAULT_MSGS, suites: list[Path]|None=None, pipeline_version: str = rtdaas.__version__) -> dict
#   decode bags (mcap_lite + CdrCodec) -> raw tables; sessions & kpi_1m via rtdaas.metrics; JIGSAWS -> procedures.* ;
#   run quality suites -> dq.check_results ; write catalog table ; returns row counts per table
# cli.py -> console script `rtdaas-local`
#   pipeline [--bags DIR] [--jigsaws DIR] --out DB [--suite FILE ...]
```

### 4.8 `rtdaas.spark` (requires pyspark; imported lazily)

```python
# common.py
def build_spark(app_name: str, settings: Settings|None=None) -> SparkSession   # Iceberg REST catalog "lake", s3a creds, Kafka/Avro packages via spark.jars.packages
def ensure_tables(spark) -> None            # CREATE TABLE IF NOT EXISTS for every §3.2 table (DDL in rtdaas/spark/ddl.py)
def write_iceberg(df, table: str, mode: str = "append") -> None           # table like "telemetry.sessions"
def merge_iceberg(spark, df, table: str, keys: list[str]) -> None         # MERGE INTO dedupe upsert via temp view
def es_bulk(rows: Iterable[dict], index: str, id_fn: Callable[[dict], str], settings) -> int   # elasticsearch-py helpers.bulk
def strip_wire_header(col)                   # F.expr("substring(value, 6, length(value)-5)")
# streaming_kpis.py   spark-submit rtdaas/spark/streaming_kpis.py [--trigger "30 seconds"]
# batch_mcap.py       spark-submit rtdaas/spark/batch_mcap.py --date YYYY-MM-DD [--robot ID] [--input s3a://rtdaas-raw/bags]
# batch_jigsaws.py    spark-submit rtdaas/spark/batch_jigsaws.py --root s3a://rtdaas-raw/jigsaws [--task Suturing]
# quality_gate.py     spark-submit rtdaas/spark/quality_gate.py --suite quality/checks/telemetry.yml --partition YYYY-MM-DD   exit 2 on blocking failures
# publish_marts.py    spark-submit rtdaas/spark/publish_marts.py --date YYYY-MM-DD[,YYYY-MM-DD...] [--marts SCHEMA.TABLE,...]
```

### 4.9 `rtdaas.airflow.dags`

| DAG id                 | Schedule       | Tasks                                                                                   |
|------------------------|----------------|-----------------------------------------------------------------------------------------|
| `telemetry_bag_ingest` | `@daily`       | wait_for_bags (S3 prefix sensor) → spark_batch_mcap → quality_gate → publish_marts → index_es_sessions |
| `telemetry_backfill`   | manual, params `start_date`, `end_date`, `robot_id` | expand over dates: batch_mcap → quality_gate; then publish_marts for every date |
| `jigsaws_ingest`       | manual / on dataset drop | discover → spark_batch_jigsaws → quality_gate(procedures) → publish_marts → index_es_gestures |
| `dq_daily`             | `0 6 * * *`    | run all suites against yesterday's partitions → write dq.check_results → notify on failure |
| `contracts_sync`       | on deploy      | rtdaas-contracts check (vs registry) → register                                          |

Spark tasks use `SparkKubernetesOperator` (k8s) or `SparkSubmitOperator` (compose), selected
by Airflow Variable `rtdaas_spark_mode` ∈ {`k8s`, `submit`}. Connection ids: `rtdaas_s3`, `spark_default`.
The API triggers runs via Airflow REST `POST /api/v1/dags/{dag_id}/dagRuns`.

### 4.10 `rtdaas.serving`

Starlette app factory `create_app(settings) -> Starlette`; console script `rtdaas-api` runs uvicorn.
Backend protocol (`rtdaas/serving/backends/base.py`):

```python
class Backend(Protocol):
    def catalog(self) -> list[dict]
    def robots(self) -> list[dict]                                   # robot_id, first_seen, last_seen, n_sessions
    def sessions(self, robot_id=None, start=None, end=None, limit=200, offset=0) -> list[dict]
    def session(self, session_id) -> dict|None                       # includes per-arm rows
    def trajectory(self, session_id, arm, downsample: int = 1) -> list[dict]   # ts,x,y,z,jaw_angle,speed
    def session_kpis(self, session_id) -> list[dict]
    def events(self, q=None, robot_id=None, severity_min=None, start=None, end=None, limit=200) -> list[dict]
    def trials(self, task=None, skill_level=None, subject=None) -> list[dict]
    def trial(self, trial_id) -> dict|None
    def trial_kinematics(self, trial_id, arms: list[str], downsample: int = 1) -> list[dict]
    def trial_frames(self, trial_id) -> list[dict]
    def gestures(self, task=None, gesture=None, skill_level=None, limit=500) -> list[dict]
    def quality_runs(self, limit=50) -> list[dict]
    def quality_results(self, run_id=None, table_name=None, status=None, limit=500) -> list[dict]
    def contracts(self) -> list[dict]                                 # subject, version, schema (from registry or contracts/avro)
    def record_job(self, kind, target, dag_run_id, status) -> dict
    def jobs(self, limit=50) -> list[dict]
```

Implementations: `SqliteBackend` (local mirror), `DuckDBBackend` (Iceberg tables via DuckDB
`iceberg` extension or Parquet export; events/gestures optionally from Elasticsearch when `ES_URL` set).

HTTP routes (all JSON; see `openapi.yaml`):

```
GET  /api/health                      GET /api/catalog                 GET /api/contracts
GET  /api/robots                      GET /api/sessions                GET /api/sessions/{id}
GET  /api/sessions/{id}/trajectory    GET /api/sessions/{id}/kpis      GET /api/events
GET  /api/trials                      GET /api/trials/{id}             GET /api/trials/{id}/kinematics
GET  /api/trials/{id}/frames          GET /api/trials/{id}/video       GET /api/gestures
GET  /api/quality/runs                GET /api/quality/results         GET /api/jobs
POST /api/jobs/reprocess  {"kind":"session"|"trial"|"date","target":"..."}
GET  /openapi.json   GET /docs (Swagger UI)   GET /metrics (Prometheus text)   GET /  (UI)
```

UI (vanilla JS + Plotly from CDN, served from `rtdaas/serving/static/`): Sessions, Session detail
(3-D/2-D trajectory replay, KPI timeline, events, Reprocess button), Log explorer, Procedures (trials),
Trial detail (video synchronized with kinematics scrubber + gesture ribbon + frame features), Gesture search,
Catalog, Quality, Contracts.

---

## 5. Pipelines

### 5.1 Streaming (`streaming_kpis.py`)
Kafka `tool_pose`, `events`, `heartbeat` → strip wire header → `from_avro` with envelope schema
fetched from the registry at startup → flatten → watermark 30 s → per (robot_id, arm) 1-minute
tumbling windows → `kpi_1m` row (computed_by = `stream`) → `foreachBatch`: append raw rows to
`telemetry.*_raw` (source = `stream`), upsert KPI rows into `telemetry.kpi_1m`, bulk-index KPIs and
events to Elasticsearch. Checkpoint `s3a://rtdaas-lake/checkpoints/streaming_kpis`. Trigger 30 s.

### 5.2 Batch bags (`batch_mcap.py`)
List `bags/*/<yyyy>/<mm>/<dd>/*.mcap` for `--date` → `sc.binaryFiles` → `mapPartitions(decode_bag)`
(uses `mcap_lite` + `CdrCodec`, pure Python, tested) → per-topic DataFrames → `merge_iceberg` into raw tables
(keys: robot_id, arm, ts) with source = `bag` (bag rows win over stream rows for the same key) →
`groupBy(robot_id, arm, session)` `applyInPandas(session_metrics)` → `telemetry.sessions` →
`kpi_windows` per session → `kpi_1m` (computed_by = `batch`, overwrites stream rows for the same window) →
ES `rtdaas-sessions`.

### 5.3 JIGSAWS (`batch_jigsaws.py`)
`discover_trials` on the driver → `parallelize(trials)` → `mapPartitions`: parse kinematics +
transcription, `label_frames`, `frame_features` (OpenCV on executors), `gesture_metrics` →
`procedures.trials/kinematics/gestures/frame_features` → ES `rtdaas-gestures`.

### 5.4 Quality gate (`quality_gate.py`)
`run_suite(load_suite(f), SparkAdapter(spark), partition_value)` → append `dq.check_results` →
exit code 2 if any blocking check failed, so the Airflow task fails and downstream publish is skipped.

### 5.5 Publish (`publish_marts.py`)
Spark SQL builds the marts (§3.5) → `lake.marts.<table>` (dynamic partition overwrite of the
published day, or a full replace for unpartitioned marts) → catalog refresh (§6).

---

## 6. Data catalog (discoverability)

`rtdaas/serving/catalog.yml` describes every published table: `name, owner, description, sla_hours,
contract (subject or "derived"), pii: false, columns: [{name, type, description}]`. The local pipeline
and `publish_marts.py` write the `catalog` table (`name, owner, description, sla_hours, row_count,
last_updated, freshness_ok`) that `GET /api/catalog` serves. The Catalog UI page renders it.

---

## 7. Infrastructure

- **Compose (laptop)**: `infra/compose/docker-compose.yml` with profiles `core` (kafka, apicurio, minio,
  iceberg-rest, spark-master, spark-worker×2, airflow, postgres, elasticsearch, api), `demo` (sim,
  streaming job), `observability` (prometheus, grafana, kafka-exporter). `make demo` = up + init + sim.
- **VMs**: Terraform `envs/local` (libvirt, 1 server + 3 agents, cloud-init) or `envs/aws` (EC2 + S3
  buckets + IAM) → outputs Ansible inventory → `ansible-playbook site.yml` installs k3s and Argo CD →
  Argo CD app-of-apps (`infra/k8s/argocd/root.yaml`) deploys platform charts and `rtdaas` manifests.
- **Kubernetes components**: Strimzi (Kafka KRaft, 3 brokers), Apicurio Registry, Kubeflow spark-operator,
  Airflow (official chart, KubernetesExecutor, git-sync DAGs), ECK Elasticsearch, MinIO, Iceberg REST
  catalog, kube-prometheus-stack. `rtdaas` manifests: api Deployment+Service+Ingress, streaming
  `SparkApplication`, sim Deployment (demo), bridge Deployment, ConfigMap/Secret templates, ServiceMonitors.
- Container images: `ghcr.io/<owner>/rtdaas-{api,spark,airflow,sim,bridge}:<git-sha>`.

---

## 8. CI/CD (GitHub Actions)

| Workflow          | Trigger            | Jobs                                                                                     |
|-------------------|--------------------|------------------------------------------------------------------------------------------|
| `ci.yml`          | push, PR           | lint (ruff), unit tests (`pytest -m "not spark and not airflow and not integration"`), spark tests (`pip install .[spark]`, `pytest -m spark`), airflow tests (DAG import + `dag.test()` with mocks), docs build |
| `contracts.yml`   | PR touching contracts/ | `rtdaas-contracts gen` → `git diff --exit-code`; `rtdaas-contracts check --base origin/main --mode BACKWARD` |
| `images.yml`      | push to main, tags | buildx + push 5 images to GHCR                                                            |
| `integration.yml` | nightly, manual    | kind cluster → deploy Strimzi+Apicurio+MinIO+API → run sim for 60 s → assert KPIs appear via API |
| `deploy.yml`      | after images on main | bump image tags in `infra/k8s/manifests/rtdaas/kustomization.yaml` (Argo CD syncs)      |

---

## 9. Configuration (`rtdaas.config.Settings`, env vars, prefix none)

| Variable                 | Default                                   | Used by                 |
|--------------------------|-------------------------------------------|-------------------------|
| `KAFKA_BOOTSTRAP`        | `localhost:9092`                          | sim, streaming, bridge  |
| `SCHEMA_REGISTRY_URL`    | `http://localhost:8081/apis/ccompat/v7`   | all producers/consumers |
| `ES_URL`                 | `http://localhost:9200`                   | spark, api              |
| `S3_ENDPOINT`            | `http://localhost:9000`                   | spark, airflow, api     |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` / `minioadmin`      |                         |
| `S3_BUCKET_RAW` / `S3_BUCKET_LAKE` | `rtdaas-raw` / `rtdaas-lake`    |                         |
| `ICEBERG_REST_URI`       | `http://localhost:8181`                   | spark, api (duckdb)     |
| `ICEBERG_WAREHOUSE`      | `s3a://rtdaas-lake/warehouse`             | spark                   |
| `AIRFLOW_API_URL`        | `http://localhost:8080/api/v1`            | api                     |
| `AIRFLOW_USER` / `AIRFLOW_PASSWORD` | `airflow` / `airflow`          | api                     |
| `SERVING_BACKEND`        | `sqlite`                                  | api                     |
| `SERVING_SQLITE_PATH`    | `./data/rtdaas.sqlite`                    | api, local pipeline     |
| `SERVING_DUCKDB_PATH`    | `./data/rtdaas.duckdb`                    | api                     |
| `MEDIA_ROOT`             | `./data`                                  | api (trial video files) |
| `API_HOST` / `API_PORT`  | `0.0.0.0` / `8080`                        | api                     |
| `PIPELINE_VERSION`       | `rtdaas.__version__`                      | jobs                    |
| `LOG_LEVEL`              | `INFO`                                    | all                     |

---

## 10. Conventions

- Python: type hints, `from __future__ import annotations`, logging via `logging.getLogger(__name__)`,
  no print in library code. Tests in `tests/` mirror module paths; markers `spark`, `airflow`,
  `integration`, `slow`. Fixtures in `tests/fixtures/` (tiny MCAP, JIGSAWS mini dataset generated at test time).
- Timestamps are UTC everywhere; ROS `Time` → `pd.Timestamp(sec*1e9+nanosec, unit="ns", tz="UTC")`.
- Every CLI exits 0 on success, 1 on usage/runtime error, 2 on "data failed a gate".
- Container images run as non-root; config via env only; `/healthz` on every long-running service.
