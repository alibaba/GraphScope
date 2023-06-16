
# add subdirectories first
add_subdirectory(graph_db)
add_subdirectory(bsp)
message(STATUS "BUILD_HQPS: ${BUILD_HQPS}")
if (BUILD_HQPS)
    add_subdirectory(hqps)
endif()

