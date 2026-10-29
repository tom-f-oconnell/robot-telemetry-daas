# Periodic liveness message, one per robot, published at 1 Hz.
std_msgs/Header header
string robot_id
# Seconds since the robot's software stack started.
float64 uptime_s
# Coarse operating state: "idle", "homing", "ready", "teleop", "fault".
string state
# Arms currently enabled on this robot.
string[] active_arms
# Software version string of the publishing stack.
string software_version
