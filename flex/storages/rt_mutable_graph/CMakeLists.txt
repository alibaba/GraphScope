
# find yaml-cpp
find_package(yaml-cpp REQUIRED)
include_directories(SYSTEM ${yaml-cpp_INCLUDE_DIRS})

file(GLOB_RECURSE RT_MUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(flex_rt_mutable_graph SHARED ${RT_MUTABLE_GRAPH_SRC_FILES})
target_link_libraries(flex_rt_mutable_graph ${LIBGRAPELITE_LIBRARIES} ${YAML_CPP_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS flex_rt_mutable_graph
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/mutable_property_fragment.h
              ${CMAKE_CURRENT_SOURCE_DIR}/schema.h
              ${CMAKE_CURRENT_SOURCE_DIR}/mutable_csr.h
              ${CMAKE_CURRENT_SOURCE_DIR}/types.h
        DESTINATION include/flex/storages/rt_mutable_graph)
