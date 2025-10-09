vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO apache/datasketches-cpp
    REF dddc4a668cdc47ad8a221cf7d4cb5054e53a40ee
    SHA512 2fff76d978acecccadcf712389bc1b724569ab7f70c256473f59680f04473a7ffd6b1ed94125f6665bfad31a22874a18ea73d455eda49ff3e9f8a994548c52d5
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
