set(GIE_COMPILER_PROTO_DIR  ${CMAKE_CURRENT_SOURCE_DIR}/../../../interactive_engine/executor/ir/proto/)
set(GIE_COMPILER_PROTO_JOB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../interactive_engine/executor/engine/pegasus/server/proto/)
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

add_library(hqps_plan_proto SHARED ${PROTO_SRCS_GIE})
target_include_directories(hqps_plan_proto PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
target_link_libraries(hqps_plan_proto PUBLIC ${Protobuf_LIBRARIES})
install_flex_target(hqps_plan_proto)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/flex/engines/
        FILES_MATCHING
        PATTERN "*.h"
)
#install proto_generated_files
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/flex/proto_generated_gie/
        DESTINATION include/flex/proto_generated_gie/
        FILES_MATCHING
        PATTERN "*.h"
)
