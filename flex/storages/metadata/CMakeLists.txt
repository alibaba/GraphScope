
file(GLOB_RECURSE METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(flex_metadata_store SHARED ${METADATA_SRC_FILES})
target_link_libraries(flex_metadata_store flex_utils)
install_flex_target(flex_metadata_store)




