# The error code of FLEX Interactive is defined in $GRAPHSCOPE_HOME/proto/error/interactive.proto
set(GRAPHSCOPE_ERROR_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/error/)
# proto files of gie compiler
set(INTERACTIVE_ERROR_PROTO_FILES
        ${GRAPHSCOPE_ERROR_PROTO_DIR}/interactive.proto
)
#create directory first
file(MAKE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/error_pb)

# proto gen for gie proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR ERROR_PROTO_SRCS
        PROTOS ${INTERACTIVE_ERROR_PROTO_FILES}
        IMPORT_DIRS ${GRAPHSCOPE_ERROR_PROTO_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/error_pb
)
file(GLOB_RECURSE UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")


add_library(flex_utils SHARED ${UTILS_SRC_FILES} ${ERROR_PROTO_SRCS} ${Protobuf_LIBRARIES})
target_include_directories(flex_utils PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
# Link the static library of arrow, to save the trouble of linking the shared library of arrow
target_link_libraries(flex_utils ${ARROW_LIB})


target_link_libraries(flex_utils ${YAML_CPP_LIBRARIES} ${Boost_LIBRARIES} ${Protobuf_LIBRARIES})

install_flex_target(flex_utils)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/
        FILES_MATCHING
        PATTERN "*.h"
)
