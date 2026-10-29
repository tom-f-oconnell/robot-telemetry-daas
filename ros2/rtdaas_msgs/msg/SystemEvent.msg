# A discrete system event emitted by the fleet_monitor node (or the simulator).
uint8 SEVERITY_DEBUG=0
uint8 SEVERITY_INFO=1
uint8 SEVERITY_WARN=2
uint8 SEVERITY_ERROR=3
uint8 SEVERITY_FATAL=4

std_msgs/Header header
# Arm the event relates to, or "" for system-wide events.
string arm
uint8 severity
# Machine-readable code, e.g. SESSION_START, SESSION_END, JOINT_LIMIT_WARN,
# INSTRUMENT_SWAP, CLUTCH_ENGAGED, ESTOP, HEARTBEAT_LOST.
string code
# Human-readable detail.
string message
