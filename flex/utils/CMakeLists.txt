############################################ Generate Error Code Proto ############################################
# The error code of FLEX Interactive is defined in $GRAPHSCOPE_HOME/proto/error/interactive.proto
set(GRAPHSCOPE_ERROR_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/error/)
# proto files of gie compiler
set(INTERACTIVE_ERROR_PROTO_FILES
        ${GRAPHSCOPE_ERROR_PROTO_DIR}/interactive.proto
)
#create directory first
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/flex/utils/error_pb)

# proto gen for gie proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR ERROR_PROTO_SRCS
        PROTOS ${INTERACTIVE_ERROR_PROTO_FILES}
        IMPORT_DIRS ${GRAPHSCOPE_ERROR_PROTO_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/flex/utils/error_pb
)


############################################ Generate Physical Plan Proto ############################################
set(GIE_COMPILER_PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../interactive_engine/executor/ir/proto/)
set(GIE_COMPILER_PROTO_JOB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../interactive_engine/executor/engine/pegasus/server/proto/)
# proto files of gie compiler
set(CODE_GEN_PROTOBUF_FILES
       ${GIE_COMPILER_PROTO_DIR}/algebra.proto
        ${GIE_COMPILER_PROTO_DIR}/common.proto
        ${GIE_COMPILER_PROTO_DIR}/expr.proto
        ${GIE_COMPILER_PROTO_DIR}/physical.proto
        ${GIE_COMPILER_PROTO_DIR}/results.proto
        ${GIE_COMPILER_PROTO_DIR}/schema.proto
        ${GIE_COMPILER_PROTO_DIR}/type.proto
        ${GIE_COMPILER_PROTO_DIR}/stored_procedure.proto
        ${GIE_COMPILER_PROTO_JOB_DIR}/job_service.proto
)

#create directory first
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/flex/proto_generated_gie)

# proto gen for gie proto
protobuf_generate(APPEND_PATH
        TARGET ${LOCAL_EXE_NAME}
        LANGUAGE cpp
        OUT_VAR PROTO_SRCS_GIE
        PROTOS ${CODE_GEN_PROTOBUF_FILES}
        IMPORT_DIRS ${GIE_COMPILER_PROTO_DIR}
        PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/flex/proto_generated_gie
)

add_library(flex_plan_proto SHARED ${PROTO_SRCS_GIE})
target_include_directories(flex_plan_proto PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
target_link_libraries(flex_plan_proto PUBLIC ${Protobuf_LIBRARIES})
install_flex_target(flex_plan_proto)

#install proto_generated_files
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/flex
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)

############################################ Build Utils Library ############################################
file(GLOB_RECURSE UTILS_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(flex_utils SHARED ${UTILS_SRC_FILES} ${ERROR_PROTO_SRCS})
target_include_directories(flex_utils PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
# Link the static library of arrow, to save the trouble of linking the shared library of arrow
target_link_libraries(flex_utils ${ARROW_LIB} ${YAML_CPP_LIBRARIES} ${Boost_LIBRARIES} ${GLOG_LIBRARIES})
install_flex_target(flex_utils)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/
        FILES_MATCHING
        PATTERN "*.h"
)
