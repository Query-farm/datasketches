vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO apache/datasketches-cpp
    REF fbb78f4a12670c00bd74d225ba589228ce99c832
    SHA512 a08057d2ff91ea1fa5b11b0f6fe4943976f19dbf3e887a293695db3d28f824fae106955c1123570c924f69c00303a02c59f328117db018181b66e992c58e00f2
    HEAD_REF master
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
