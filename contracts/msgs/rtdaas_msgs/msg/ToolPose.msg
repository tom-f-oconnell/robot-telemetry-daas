# Cartesian pose of an instrument tip plus jaw opening, composed by the
# rtdaas tool_pose_adapter node from dVRK measured_cp + jaw/measured_js.
std_msgs/Header header
# Arm name as used in the dVRK namespace, e.g. "PSM1", "PSM2", "ECM", "MTML", "MTMR".
string arm
geometry_msgs/Point position
geometry_msgs/Quaternion orientation
# Jaw opening angle in radians (NaN for arms without a jaw).
float64 jaw_angle
# Cartesian linear speed of the tip in m/s, as estimated by the adapter.
float64 speed
