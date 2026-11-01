// 3-D replay of a da Vinci trial or session, posed by /api/.../simulation. Loaded on demand from
// app.js, because three.js comes from a CDN (see index.html): without it the rest of the UI works.
import * as THREE from "three";
import { GLTFLoader } from "three/addons/loaders/GLTFLoader.js";
import { OrbitControls } from "three/addons/controls/OrbitControls.js";

const PI_2 = Math.PI / 2;
const MODEL_URL = "/static/models/psm.glb";
const TRAIL_SECONDS = 4;
const ARM_COLORS = { PSML: 0xd85a30, PSM2: 0xd85a30, PSMR: 0x1d9e75, PSM1: 0x1d9e75, PSM3: 0x534ab7, ECM: 0x8a8a84 };

// Classic PSM with a Large Needle Driver, from dvrk_model's URDF, every joint turning or sliding
// along z: [link, parent, xyz, rpy, joint, multiplier, prismatic]. rtdaas/metrics/da_vinci.py solves
// for these same joints. Multipliers are the parallelogram linkage and the two jaws.
const LINKS = [
  ["yaw", "base", [0, 0, 0], [0, -PI_2, PI_2], "yaw"],
  ["pitch", "yaw", [0, 0, 0], [-PI_2, -PI_2, 0], "pitch"],
  ["pitch_2", "yaw", [0, 0.0295, 0.5185], [-PI_2, -PI_2, 0], "pitch"],
  ["pitch_3", "yaw", [0, 0.0295, 0.4285], [-PI_2, -PI_2, 0], "pitch"],
  ["pitch_4", "pitch_2", [0.04178, 0.15007, -0.0137], [0, 0, 0], "pitch", -1],
  ["pitch_5", "pitch_2", [0.04209, 0.18695, -0.02412], [0, 0, 0], "pitch", -1],
  ["insertion", "pitch_4", [-0.52, 0, -0.0155], [0, 0, 0], "pitch", 1],
  ["adaptor", "pitch", [0, 0.4318, 0], [PI_2, 0, 0], "insertion", 1, true],
  ["housing", "adaptor", [0, 0, 0], [0, 0, 0]],
  ["roll", "adaptor", [0, 0, 0.4162], [0, 0, 0], "roll"],
  ["wrist_pitch", "roll", [0, 0, 0], [-PI_2, -PI_2, 0], "wrist_pitch"],
  ["wrist_yaw", "wrist_pitch", [0.0091, 0, 0], [-PI_2, -PI_2, 0], "wrist_yaw"],
  ["jaw_1", "wrist_yaw", [0, 0, 0], [0, 0, 0], "jaw", 0.5],
  ["jaw_2", "wrist_yaw", [0, 0, 0], [0, 0, 0], "jaw", -0.5],
  ["tool_tip", "wrist_yaw", [0, 0.0102, 0], [0, PI_2, PI_2]],
];
const FINISH = {
  housing: { color: 0x34363a, roughness: 0.55, metalness: 0.2 },
  roll: { color: 0x26282c, roughness: 0.45, metalness: 0.5 },
  // Not very metallic: there is no environment to reflect, and these parts go black and speckled.
  wrist_pitch: { color: 0xa8abb0, roughness: 0.5, metalness: 0.25 },
  wrist_yaw: { color: 0xa8abb0, roughness: 0.5, metalness: 0.25 },
  jaw_1: { color: 0xd2d4d8, roughness: 0.45, metalness: 0.25 },
  jaw_2: { color: 0xd2d4d8, roughness: 0.45, metalness: 0.25 },
};
const ARM_FINISH = { color: 0xd9dbdc, roughness: 0.6, metalness: 0.1 };

let modelPromise = null;
function loadModel() {
  modelPromise ||= new GLTFLoader().loadAsync(MODEL_URL).then((gltf) => {
    const parts = {};
    gltf.scene.traverse((o) => { if (o.isMesh) parts[o.name] = o.geometry; });
    return parts;
  });
  return modelPromise;
}

function buildPsm(parts) {
  const root = new THREE.Group();
  const links = { base: root };
  const joints = [];
  const attach = (name, group) => {
    if (!parts[name]) return;
    const mesh = new THREE.Mesh(parts[name], new THREE.MeshStandardMaterial(FINISH[name] || ARM_FINISH));
    mesh.castShadow = true;
    group.add(mesh);
  };
  attach("base", root);
  for (const [name, parent, xyz, rpy, joint, multiplier = 1, prismatic = false] of LINKS) {
    const group = new THREE.Group();
    group.matrixAutoUpdate = false;
    const origin = new THREE.Matrix4().compose(
      new THREE.Vector3(...xyz),
      new THREE.Quaternion().setFromEuler(new THREE.Euler(rpy[0], rpy[1], rpy[2], "ZYX")),
      new THREE.Vector3(1, 1, 1),
    );
    group.matrix.copy(origin);
    links[parent].add(group);
    links[name] = group;
    attach(name, group);
    if (joint) joints.push({ group, origin, joint, multiplier, prismatic });
  }
  const motion = new THREE.Matrix4();
  const setJoints = (values) => {
    for (const { group, origin, joint, multiplier, prismatic } of joints) {
      const v = values[joint] * multiplier;
      if (prismatic) motion.makeTranslation(0, 0, v); else motion.makeRotationZ(v);
      group.matrix.multiplyMatrices(origin, motion);
      group.matrixWorldNeedsUpdate = true;
    }
  };
  return { root, setJoints, tip: links.tool_tip };
}

// Index of the last sample at or before t, with the fraction of the way to the next.
function locate(times, t) {
  let lo = 0, hi = times.length - 1;
  if (t <= times[0]) return [0, 0];
  if (t >= times[hi]) return [hi, 0];
  while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (times[mid] <= t) lo = mid; else hi = mid; }
  return [lo, (t - times[lo]) / (times[hi] - times[lo])];
}

function buildTrail(arm, scene) {
  const flat = new Float32Array(arm.tip.flat());
  const geometry = new THREE.BufferGeometry();
  geometry.setAttribute("position", new THREE.BufferAttribute(flat, 3));
  geometry.setDrawRange(0, 0);
  const line = new THREE.Line(geometry, new THREE.LineBasicMaterial({ color: ARM_COLORS[arm.name] ?? 0x888888 }));
  line.frustumCulled = false;
  scene.add(line);
  return (i) => {
    const dt = arm.t_s.length > 1 ? (arm.t_s[arm.t_s.length - 1] - arm.t_s[0]) / (arm.t_s.length - 1) : 1;
    const n = Math.min(i + 1, Math.round(TRAIL_SECONDS / dt));
    geometry.setDrawRange(i + 1 - n, n);
  };
}

function buildEndoscope(arm, scene) {
  const material = new THREE.MeshStandardMaterial({ color: 0x3a3d42, roughness: 0.5, metalness: 0.4 });
  const shaft = new THREE.Mesh(new THREE.CylinderGeometry(0.006, 0.006, 1, 16), material);
  scene.add(shaft);
  const pivot = new THREE.Vector3(...arm.pivot), tip = new THREE.Vector3(), up = new THREE.Vector3(0, 1, 0), dir = new THREE.Vector3();
  return (i) => {
    tip.set(...arm.tip[i]);
    // The shaft runs from the tip, through the pivot, a fixed length beyond it.
    dir.subVectors(pivot, tip).normalize();
    shaft.scale.y = 0.45;
    shaft.position.copy(tip).addScaledVector(dir, 0.225);
    shaft.quaternion.setFromUnitVectors(up, dir);
  };
}

// A schematic cart: dvrk_model has no light meshes for it, and no data poses it. A column behind the
// arms and a boom out to each arm's mount.
function buildCart(mounts, floor, scene) {
  const material = new THREE.MeshStandardMaterial({ color: 0xc9cbcd, roughness: 0.7, metalness: 0.1 });
  const center = mounts.reduce((sum, m) => sum.add(m), new THREE.Vector3()).divideScalar(mounts.length);
  const top = Math.max(...mounts.map((m) => m.z)) + 0.12;
  const foot = new THREE.Vector3(center.x, Math.max(...mounts.map((m) => m.y)) + 0.22, floor);
  const head = foot.clone().setZ(top);
  const up = new THREE.Vector3(0, 1, 0);
  const rod = (from, to, radius) => {
    const mesh = new THREE.Mesh(new THREE.CylinderGeometry(radius, radius, from.distanceTo(to), 24), material);
    mesh.position.copy(from).add(to).multiplyScalar(0.5);
    mesh.quaternion.setFromUnitVectors(up, to.clone().sub(from).normalize());
    scene.add(mesh);
  };
  rod(foot, head, 0.075);
  mounts.forEach((m) => rod(head, m, 0.035));
  const plinth = new THREE.Mesh(new THREE.BoxGeometry(0.3, 0.3, 0.03), material);
  plinth.position.copy(foot).setZ(floor + 0.02);
  scene.add(plinth);
}

export async function createRobotSim(container, data) {
  const parts = await loadModel();
  const scene = new THREE.Scene();
  scene.background = new THREE.Color(0x1b2027);
  scene.add(new THREE.HemisphereLight(0xffffff, 0x3a3f46, 1.5));
  const key = new THREE.DirectionalLight(0xffffff, 2.2);
  key.position.set(-0.8, -1.2, 2.2);
  scene.add(key);
  const fill = new THREE.DirectionalLight(0xbfd4ff, 0.7);
  fill.position.set(1.2, 0.6, 0.6);
  scene.add(fill);

  const tips = data.arms.flatMap((a) => a.tip);
  const floor = Math.min(...tips.map((p) => p[2])) - 0.02;
  const grid = new THREE.GridHelper(2, 40, 0x4a525c, 0x2d343c);
  grid.rotation.x = PI_2; // GridHelper lies in xz; the scene's up is z
  grid.position.z = floor;
  scene.add(grid);

  const updates = [];
  const psms = {};
  const mounts = [];
  for (const arm of data.arms) {
    const trail = buildTrail(arm, scene);
    if (arm.kind === "ecm") {
      const place = buildEndoscope(arm, scene);
      updates.push((t) => { const [i] = locate(arm.t_s, t); place(i); trail(i); });
      continue;
    }
    const psm = buildPsm(parts);
    psm.root.matrixAutoUpdate = false;
    psm.root.matrix.fromArray(arm.base).transpose(); // row-major from the API
    psm.root.matrixWorldNeedsUpdate = true;
    scene.add(psm.root);
    psms[arm.name] = psm;
    mounts.push(new THREE.Vector3(0, -0.6, -0.04).applyMatrix4(psm.root.matrix)); // the base link's middle
    const names = Object.keys(arm.q);
    const values = {};
    updates.push((t) => {
      const [i, f] = locate(arm.t_s, t);
      const next = Math.min(i + 1, arm.t_s.length - 1);
      for (const name of names) values[name] = arm.q[name][i] + (arm.q[name][next] - arm.q[name][i]) * f;
      psm.setJoints(values);
      trail(i);
    });
  }

  if (mounts.length) buildCart(mounts, floor, scene);

  const camera = new THREE.PerspectiveCamera(36, 1, 0.005, 20);
  camera.up.set(0, 0, 1);
  const renderer = new THREE.WebGLRenderer({ antialias: true, preserveDrawingBuffer: true });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  container.appendChild(renderer.domElement);
  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enableDamping = false;

  let dirty = true;
  const invalidate = () => { dirty = true; };
  controls.addEventListener("change", invalidate);
  const setView = (name) => {
    const view = data.views[name];
    camera.position.set(...view.eye);
    controls.target.set(...view.target);
    controls.update();
    invalidate();
  };
  const resize = () => {
    const { clientWidth: w, clientHeight: h } = container;
    if (!w || !h) return;
    renderer.setSize(w, h, false);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
    invalidate();
  };
  const observer = new ResizeObserver(resize);
  observer.observe(container);
  resize();
  setView("arms");

  let currentTime = -1;
  return {
    duration: data.duration_s,
    setTime(t) {
      if (t === currentTime) return;
      currentTime = t;
      updates.forEach((update) => update(t));
      invalidate();
    },
    setView,
    render() {
      if (!dirty) return;
      dirty = false;
      renderer.render(scene, camera);
    },
    // Where the model's tool tip is now, for checking it against the recorded one.
    tipPosition(name) {
      scene.updateMatrixWorld(true);
      return psms[name].tip.getWorldPosition(new THREE.Vector3()).toArray();
    },
    dispose() {
      observer.disconnect();
      controls.dispose();
      renderer.dispose();
    },
  };
}

// Keeps the simulation on `video` when there is a video to follow, and otherwise runs it on its own
// clock: no video, one that fails, or kinematics that continue after it ends. `ui` holds the play
// button, slider, clock and status elements.
export function runTransport(sim, ui, video, { onTime } = {}) {
  let own = !video, playing = false, t = 0, last = performance.now(), dragging = false;
  const hasVideo = () => video && !video.error && Number.isFinite(video.duration);
  const status = (text) => { ui.status.textContent = text; };
  status(video ? "Following the video" : "Playing from the recording");
  ui.slider.max = sim.duration;

  if (video) {
    video.addEventListener("error", () => { own = true; status("Video unavailable: playing from the recording"); });
    video.addEventListener("ended", () => {
      if (sim.duration > video.duration + 0.5) { own = true; t = video.duration; playing = true; status("Video ended: kinematics continue"); }
    });
    video.addEventListener("seeking", () => { if (hasVideo()) { own = false; playing = false; status("Following the video"); } });
    video.addEventListener("play", () => { if (hasVideo()) { own = false; playing = false; status("Following the video"); } });
  }
  ui.play.addEventListener("click", () => {
    if (t >= sim.duration && video && hasVideo()) {
      video.currentTime = 0;
      video.play();
    } else if (!own && hasVideo()) {
      if (video.paused) video.play(); else video.pause();
    } else {
      if (t >= sim.duration) t = 0;
      playing = !playing;
    }
  });
  const seek = (value) => {
    if (video && hasVideo() && value <= video.duration) {
      own = false;
      video.currentTime = value;
    } else {
      own = true;
      playing = false;
      t = value;
    }
  };
  ui.slider.addEventListener("input", () => { dragging = true; seek(Number(ui.slider.value)); });
  ui.slider.addEventListener("change", () => { dragging = false; });

  const frame = (now) => {
    if (!ui.root.isConnected) { sim.dispose(); return; }
    const dt = (now - last) / 1000;
    last = now;
    if (!own && hasVideo()) t = Math.min(video.currentTime, sim.duration);
    else if (playing) { t = Math.min(t + dt, sim.duration); if (t >= sim.duration) playing = false; }
    sim.setTime(t);
    sim.render();
    if (!dragging) ui.slider.value = t;
    ui.clock.textContent = `${t.toFixed(2)} / ${sim.duration.toFixed(1)} s`;
    const running = !own && hasVideo() ? !video.paused && !video.ended : playing;
    ui.play.textContent = running ? "❚❚" : "▶";
    if (onTime) onTime(t);
    requestAnimationFrame(frame);
  };
  requestAnimationFrame(frame);
  return { seek };
}
