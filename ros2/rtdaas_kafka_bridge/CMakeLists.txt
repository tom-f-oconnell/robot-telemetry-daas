cmake_minimum_required(VERSION 3.16)
project(rtdaas_kafka_bridge LANGUAGES CXX)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()
if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# --- ROS 2 ----------------------------------------------------------------------------------
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rcpputils REQUIRED)
find_package(rosbag2_cpp REQUIRED)                             # typesupport helpers on pre-Iron distros
find_package(rosidl_runtime_cpp REQUIRED)
find_package(rosidl_typesupport_cpp REQUIRED)
find_package(rosidl_typesupport_introspection_cpp REQUIRED)
find_package(std_msgs REQUIRED)

# --- third party ----------------------------------------------------------------------------
find_package(CURL REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(AvroCpp REQUIRED)                                 # cmake/FindAvroCpp.cmake

# yaml-cpp: Ubuntu 24.04 (0.8) exports yaml-cpp::yaml-cpp; older versions export yaml-cpp.
find_package(yaml-cpp REQUIRED)
if(TARGET yaml-cpp::yaml-cpp)
  set(RTDAAS_YAML_TARGET yaml-cpp::yaml-cpp)
else()
  set(RTDAAS_YAML_TARGET yaml-cpp)
endif()

# librdkafka: prefer the CMake package config (exports RdKafka::rdkafka++), fall back to pkg-config.
find_package(RdKafka CONFIG QUIET)
if(TARGET RdKafka::rdkafka++)
  set(RTDAAS_RDKAFKA_TARGET RdKafka::rdkafka++)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(RDKAFKAXX REQUIRED IMPORTED_TARGET rdkafka++)
  set(RTDAAS_RDKAFKA_TARGET PkgConfig::RDKAFKAXX)
endif()

# --- library with everything that does not need a ROS graph ---------------------------------
add_library(${PROJECT_NAME}_core
  src/avro_mapper.cpp
  src/bridge_config.cpp
  src/registry_client.cpp
  src/kafka_sink.cpp
)
target_include_directories(${PROJECT_NAME}_core PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
)
target_link_libraries(${PROJECT_NAME}_core PUBLIC
  AvroCpp::avrocpp
  CURL::libcurl
  nlohmann_json::nlohmann_json
  ${RTDAAS_YAML_TARGET}
  ${RTDAAS_RDKAFKA_TARGET}
  rclcpp::rclcpp
  ${rosidl_typesupport_introspection_cpp_TARGETS}
)

# --- node executable ------------------------------------------------------------------------
add_executable(kafka_bridge src/kafka_bridge_node.cpp)
target_link_libraries(kafka_bridge PRIVATE
  ${PROJECT_NAME}_core
  rclcpp::rclcpp
  rcpputils::rcpputils
  ${rosbag2_cpp_TARGETS}
  ${rosidl_runtime_cpp_TARGETS}
  ${rosidl_typesupport_cpp_TARGETS}
  ${rosidl_typesupport_introspection_cpp_TARGETS}
  ${std_msgs_TARGETS}
)

install(TARGETS kafka_bridge
  DESTINATION lib/${PROJECT_NAME}
)
install(TARGETS ${PROJECT_NAME}_core
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)
install(DIRECTORY config
  DESTINATION share/${PROJECT_NAME}
)

# --- tests ----------------------------------------------------------------------------------
if(BUILD_TESTING)
  find_package(ament_cmake_gtest REQUIRED)
  ament_add_gtest(test_schema_mapping test/test_schema_mapping.cpp)
  if(TARGET test_schema_mapping)
    target_link_libraries(test_schema_mapping ${PROJECT_NAME}_core)
  endif()
endif()

ament_package()
