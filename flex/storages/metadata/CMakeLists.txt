
file(GLOB_RECURSE METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
# Exclude etcd_metadata_store.cc, if BUILD_ETCD_METASTORE is off
if (NOT BUILD_ETCD_METASTORE)
    list(REMOVE_ITEM METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/etcd_metadata_store.cc")
endif()
message(STATUS "metadata source files: ${METADATA_SRC_FILES}")

add_library(flex_metadata_store SHARED ${METADATA_SRC_FILES})
target_link_libraries(flex_metadata_store flex_utils)
if (BUILD_ETCD_METASTORE)
    target_link_libraries(flex_metadata_store ${ETCD_CPP_LIBRARIES})
    # message(STATUS "etcd cpp include dir: ${ETCD_CPP_INCLUDE_DIR}")
endif()
install_flex_target(flex_metadata_store)




