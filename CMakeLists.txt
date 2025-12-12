cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME datasketches)

set(CMAKE_CXX_STANDARD 17)


if (WIN32 AND MSVC)
  add_compile_options(/bigobj)
endif()

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(DataSketches CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)


set(EXTENSION_SOURCES src/datasketches_extension.cpp src/generated.cpp
src/query_farm_telemetry.cpp
src/theta_sketch.cpp
src/frequent_items_sketch.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} datasketches)
target_link_libraries(${LOADABLE_EXTENSION_NAME} datasketches)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
