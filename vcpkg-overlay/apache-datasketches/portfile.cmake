vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO apache/datasketches-cpp
    REF 5b5524f55d121225a7909c2851271327684585d7 # 5.3.0
    SHA512 f37a85be727782afae155126198f1839d2a67dd1bb6025b46d8a84eefc5629b5ae2695208c244acccb4aa646f0fd23740d763e169103812324315643c99b3488
    HEAD_REF master
    PATCHES
        # req_sketch kept K in 8 bits, so a K of 256 or more wrapped around (256 became 4). Not yet fixed upstream.
        req-k-16-bit.patch
        # count_min_sketch drew its hash seeds with std::default_random_engine and std::uniform_int_distribution, which
        # differ between standard libraries, so sketches built on Linux, macOS and Windows disagreed. The patch
        # computes libstdc++'s seeds on every platform.
        count-min-portable-hash-seeds.patch
        # hll_union copied input sketches with their own allocator, so a union could hold memory of another allocator
        # (https://github.com/apache/datasketches-cpp/pull/556).
        hll-union-allocator.patch
        # ebpps_sketch::merge(const&) called an unqualified swap, which does not compile with DSAllocator
        # (https://github.com/apache/datasketches-cpp/pull/557).
        ebpps-merge-qualified-swap.patch
        # kll_sketch allocated K items in its constructor; it now allocates them on first use
        # (https://github.com/apache/datasketches-cpp/pull/558).
        kll-lazy-items.patch
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
