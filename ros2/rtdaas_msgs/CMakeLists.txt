cmake_minimum_required(VERSION 3.16)
project(rtdaas_msgs)

find_package(ament_cmake REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(std_msgs REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(builtin_interfaces REQUIRED)

set(msg_files
  "msg/ToolPose.msg"
  "msg/SystemEvent.msg"
  "msg/Heartbeat.msg"
)

rosidl_generate_interfaces(${PROJECT_NAME}
  ${msg_files}
  DEPENDENCIES std_msgs geometry_msgs builtin_interfaces
)

ament_export_dependencies(rosidl_default_runtime)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  # Fail the build if the ROS copies drift from the repository-level data contracts.
  add_test(
    NAME rtdaas_msgs_in_sync_with_contracts
    COMMAND bash ${CMAKE_CURRENT_SOURCE_DIR}/../check-msgs-in-sync.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..
  )
endif()

ament_package()
