
file(GLOB_RECURSE METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

if (NOT BUILD_FILE_META_STORE)
    # remove files start with local_file
    list(REMOVE_ITEM METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/local_file*")
endif()

add_library(flex_metadata_store SHARED ${METADATA_SRC_FILES})
target_link_libraries(flex_metadata_store ${YAML_CPP_LIBRARIES} flex_utils)
install_flex_target(flex_metadata_store)




