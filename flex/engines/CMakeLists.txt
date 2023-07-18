
add_subdirectory(graph_db)
add_subdirectory(bsp)
add_subdirectory(http_server)
message(STATUS "BUILD_HQPS: ${BUILD_HQPS}")
if (BUILD_HQPS)
    add_subdirectory(hqps_db)
endif()

