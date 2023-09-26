
# find yaml-cpp
find_package(yaml-cpp REQUIRED)
include_directories(SYSTEM ${yaml-cpp_INCLUDE_DIRS})

file(GLOB_RECURSE RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(flex_rt_mutable_graph SHARED ${RT_MUTABLE_GRAPH_SRC_FILES})
target_link_libraries(flex_rt_mutable_graph ${LIBGRAPELITE_LIBRARIES} ${YAML_CPP_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

if (ARROW_SHARED_LIB)
        target_link_libraries(flex_rt_mutable_graph ${ARROW_SHARED_LIB})
else()
        target_link_libraries(flex_rt_mutable_graph ${ARROW_STATIC_LIB})
endif()

install(TARGETS flex_rt_mutable_graph
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/storages
        FILES_MATCHING
        PATTERN "*.h"
)
