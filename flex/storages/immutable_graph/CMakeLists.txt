file(GLOB_RECURSE IMMUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(flex_immutable_graph SHARED ${IMMUTABLE_GRAPH_SRC_FILES})
target_link_libraries(flex_immutable_graph ${LIBGRAPELITE_LIBRARIES})
install_flex_target(flex_immutable_graph)