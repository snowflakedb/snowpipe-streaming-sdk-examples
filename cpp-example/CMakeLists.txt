# Builds against an unpacked C++ SDK release package.
# Point CMAKE_PREFIX_PATH at the package root.
cmake_minimum_required(VERSION 3.21)
project(snowpipe_streaming_cpp_example LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SnowflakeStreamingIngest REQUIRED)

foreach(quickstart elastic_channel_quickstart named_channel_quickstart)
  add_executable(${quickstart} ${quickstart}.cc)
  target_link_libraries(${quickstart} PRIVATE Snowflake::snowflake_streaming_ingest)
endforeach()
