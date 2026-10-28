# The library comes from the Query.Farm fork's duckdb-5.3.0 branch: release 5.3.0 plus the fixes below, one commit
# each, until releases include them. Drop a commit (and move REF) once its fix ships upstream.
#   apache/datasketches-cpp#552  req_sketch kept K in 8 bits (256 became 4)
#   apache/datasketches-cpp#555  count_min_sketch hash seeds differed between standard libraries
#   apache/datasketches-cpp#556  hll_union copied input sketches with their allocator
#   apache/datasketches-cpp#557  ebpps_sketch::merge(const&) called an unqualified swap
#   apache/datasketches-cpp#558  kll_sketch allocated K items in its constructor
#   apache/datasketches-cpp#559  count_min_sketch hash seeds ignored the sketch's allocator
#   apache/datasketches-cpp#560  frequent-items purge allocated a temporary sample each time
#   apache/datasketches-cpp#561  tdigest reserved its buffers up front
#   apache/datasketches-cpp#562  density_sketch did not work with a custom allocator
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO rustyconover/datasketches-cpp
    REF 44f777b89af7d81fb575c1a805432328d422bbde # duckdb-5.3.0
    SHA512 66a610ccfc7633e7e7f0afadec2963522bed16fe07886c48275e231428106517ef053ad7ef4d22bfae1a9ca48bc95a034223a7e877ea4515f34f461426f43c10
    HEAD_REF duckdb-5.3.0
)

set(VCPKG_BUILD_TYPE release) # header-only port

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DBUILD_TESTS=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(PACKAGE_NAME DataSketches CONFIG_PATH lib/DataSketches/cmake)

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/lib")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
