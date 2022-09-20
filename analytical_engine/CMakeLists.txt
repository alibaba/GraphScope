cmake_minimum_required(VERSION 3.1)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/../VERSION GRAPHSCOPE_ANALYTICAL_VERSION)
# Strip trailing newline
string(REGEX REPLACE "\n$" "" GRAPHSCOPE_ANALYTICAL_VERSION "${GRAPHSCOPE_ANALYTICAL_VERSION}")

if (POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif ()
project(analytical_engine LANGUAGES C CXX)
set(PROJECT_VERSION ${GRAPHSCOPE_ANALYTICAL_VERSION})

option(NETWORKX "networkx on?" ON)
option(BUILD_TESTS "Build unit test" ON)
option(ENABLE_JAVA_SDK "Build with support for java sdk" ON)
option(LET_IT_CRASH_ON_EXCEPTION "Disable boost leaf's error handling and let it crash when exception occurs to help debugging" OFF)

# Get system processor
execute_process(COMMAND uname -m OUTPUT_VARIABLE SYSTEM_PROCESSOR)
string(REGEX REPLACE "\n$" "" SYSTEM_PROCESSOR "${SYSTEM_PROCESSOR}")

if (ENABLE_JAVA_SDK)
    #check whether we are using clang.
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(STATUS "Using Clang compiler: ${CMAKE_CXX_COMPILER}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fforce-emit-vtables -flto")
    else()
        message(WARNING "Compiling with ENABLE_JAVA_SDK ON expects a minimum Clang-11 compiler, "
                        "current compiler is ${CMAKE_CXX_COMPILER}. The build process will continue, "
                        "BUT llvm4jni-based acceleration will be unavailable for generated libs.")
    endif()
endif()

if (NETWORKX)
    add_definitions(-DNETWORKX)
    add_definitions(-DRAPIDJSON_HAS_CXX11=1)
    add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)
    add_definitions(-DRAPIDJSON_HAS_CXX11_RVALUE_REFS=1)
    add_definitions(-DRAPIDJSON_HAS_CXX11_RANGE_FOR=1)
endif ()

set(DEFAULT_BUILD_TYPE "Release")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
    set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

find_program(ccache_EXECUTABLE ccache)
if (ccache_EXECUTABLE)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${ccache_EXECUTABLE})
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${ccache_EXECUTABLE})
    add_custom_target(ccache-stats COMMAND ${ccache_EXECUTABLE} --show-stats)
else ()
    add_custom_target(ccache-stats COMMAND echo "ccache not found.")
endif (ccache_EXECUTABLE)

# enable colored diagnostics
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-fdiagnostics-color=always)
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    add_compile_options(-fcolor-diagnostics)
endif()

include(CheckCXXCompilerFlag)
include(CheckLibraryExists)
include(CheckCXXSourceRuns)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# include /usr/local/lib as cmake installed dependencies will be placed there by default
include_directories(SYSTEM /usr/local/include)

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/proto)

# Set flags
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99")
if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
else ()
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,$ORIGIN")
endif ()
check_cxx_compiler_flag(-Wno-class-memaccess W_NO_CLASS_MEMACCESS)
check_cxx_compiler_flag(-Wno-redundant-move W_NO_REDUNDANT_MOVE)
check_cxx_compiler_flag(-Wno-undef-prefix W_NO_UNDEF_PREFIX)
if(W_NO_CLASS_MEMACCESS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess")
endif()
if(W_NO_REDUNDANT_MOVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-redundant-move")
endif()
if (W_NO_UNDEF_PREFIX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-undef-prefix")
endif()
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")
if (NOT APPLE)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage")
endif ()

set(CMAKE_THREAD_PREFER_PTHREAD ON)

include(GNUInstallDirs)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib:${CMAKE_INSTALL_PREFIX}/lib64")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

find_package(Threads REQUIRED)
find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

# put `-fopenmp` after `find_package(MPI)` as `MPI_CXX_COMPILER` cannot find it correctly
if (APPLE)
    if (POLICY CMP0056)
        cmake_policy(SET CMP0056 NEW)
    endif ()
    if (SYSTEM_PROCESSOR MATCHES "arm64")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L/opt/homebrew/lib")
        if(CMAKE_C_COMPILER_ID MATCHES "Clang")
            set(OpenMP_C "${CMAKE_C_COMPILER}" CACHE STRING "" FORCE)
            set(OpenMP_C_FLAGS "-fopenmp=libomp -Wno-unused-command-line-argument" CACHE STRING "" FORCE)
            set(OpenMP_C_LIB_NAMES "libomp" CACHE STRING "" FORCE)
            set(OpenMP_libomp_LIBRARY "omp" CACHE STRING "" FORCE)
        endif()
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            set(OpenMP_CXX "${CMAKE_CXX_COMPILER}" CACHE STRING "" FORCE)
            set(OpenMP_CXX_FLAGS "-fopenmp=libomp -Wno-unused-command-line-argument" CACHE STRING "" FORCE)
            set(OpenMP_CXX_LIB_NAMES "libomp" CACHE STRING "" FORCE)
            set(OpenMP_libomp_LIBRARY "omp" CACHE STRING "" FORCE)
        endif()
    else()
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L/usr/local/lib")
    endif()
endif ()
find_package(OpenMP REQUIRED)

find_package(Boost REQUIRED COMPONENTS system filesystem
             # required by folly
             context program_options regex thread)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
# eliminate a lot of warnings for newer version of boost library.
add_compile_options(-DBOOST_BIND_GLOBAL_PLACEHOLDERS)

# check the gcc abi
include("cmake/CheckGCCABICompatibility.cmake")
check_gcc_compatible()

include("cmake/FindGFlags.cmake")
if (GFLAGS_FOUND)
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "gflags not found")
endif ()

# api changed in glog 0.6.0
include("cmake/FindGlog.cmake")
include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
if (GLOG_FOUND)
    set(CMAKE_REQUIRED_INCLUDES "${GLOG_INCLUDE_DIRS}")
    set(CMAKE_REQUIRED_LIBRARIES "${CMAKE_REQUIRED_LIBRARIES} ${GLOG_LIBRARIES}")
    check_cxx_source_runs("
    #include <iostream>
    #include <glog/logging.h>

    int main() {
        google::LogMessageTime time;
    }
    " GLOG_COMPILE_SUCCESS)
    if (GLOG_COMPILE_SUCCESS)
        add_definitions(-DGLOG_VERSION=6)
    endif ()
endif ()


include("cmake/FindArrow.cmake")
if (ARROW_FOUND)
    include_directories("${ARROW_INCLUDE_DIR}")
else ()
    message(FATAL_ERROR "arrow not found")
endif ()

include("cmake/FindLibUnwind.cmake")
if (${LIBUNWIND_FOUND})
    add_definitions(-DWITH_LIBUNWIND)
endif ()

find_package(libgrapelite REQUIRED)
include_directories(${LIBGRAPELITE_INCLUDE_DIRS})

find_package(vineyard 0.8.5 REQUIRED)
include_directories(${VINEYARD_INCLUDE_DIRS})
add_compile_options(-DENABLE_SELECTOR)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

if (ENABLE_JAVA_SDK)
# find jni---------------------------------------------------------------------
    find_package(JNI REQUIRED)
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
    include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})
endif()

include("cmake/FindGRPC.cmake")
include_directories(${GRPC_INCLUDE_DIR})

# Generate proto
execute_process(COMMAND python3 python/graphscope/proto/proto_generator.py "${PROJECT_SOURCE_DIR}" --cpp
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/..)

file(GLOB PROTO_CPP_FILES "graphscope/proto/*.cc")
file(GLOB CORE_RPC_SRC_FILES "core/server/*.cc")

# Add targets
add_library(gs_proto SHARED ${PROTO_CPP_FILES})
set_source_files_properties(${PROTO_CPP_FILES} PROPERTIES COMPILE_FLAGS -Wno-array-bounds)
target_include_directories(gs_proto PRIVATE "${PROJECT_SOURCE_DIR}/proto")
target_link_libraries(gs_proto
        ${Protobuf_LIBRARIES}
        ${GRPC_LIBRARY}
        ${GRPC_GRPC++_LIBRARY}
        ${GRPC_GRPC++_REFLECTION_LIBRARY}
    )

add_library(gs_util SHARED "core/flags.cc" "core/launcher.cc")
target_link_libraries(gs_util PRIVATE
        ${LIBGRAPELITE_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${VINEYARD_LIBRARIES}
        ${Boost_LIBRARIES}
    )

add_executable(grape_engine
        "core/grape_engine.cc"
        "core/grape_instance.cc"
        "core/java/javasdk.cc"
        "core/object/dynamic.cc"
        ${CORE_RPC_SRC_FILES}
    )
target_include_directories(grape_engine PRIVATE proto utils)

if (ENABLE_JAVA_SDK)
    target_compile_definitions(grape_engine PUBLIC ENABLE_JAVA_SDK)
    target_link_libraries(grape_engine PRIVATE
        gs_proto
        gs_util
        ${LIBGRAPELITE_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${CMAKE_DL_LIBS}
        ${Boost_LIBRARIES}
        ${VINEYARD_LIBRARIES}
        ${JAVA_JVM_LIBRARY}
        ${JNI_LIBRARIES}
        )
else()
    target_link_libraries(grape_engine PRIVATE
        gs_proto
        gs_util
        ${LIBGRAPELITE_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${CMAKE_DL_LIBS}
        ${Boost_LIBRARIES}
        ${VINEYARD_LIBRARIES}
        )
endif()
if (LET_IT_CRASH_ON_EXCEPTION)
    target_compile_definitions(grape_engine PRIVATE LET_IT_CRASH_ON_EXCEPTION)
endif ()
target_link_libraries(grape_engine PRIVATE OpenMP::OpenMP_CXX)

if (${LIBUNWIND_FOUND})
    target_link_libraries(grape_engine PRIVATE ${LIBUNWIND_LIBRARIES})
endif ()

# Test targets
if (BUILD_TESTS)
    add_executable(run_app test/run_app.cc core/object/dynamic.cc)
    target_include_directories(run_app PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps utils apps)
    target_link_libraries(run_app ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${CMAKE_DL_LIBS} ${Boost_LIBRARIES} ${VINEYARD_LIBRARIES})
    target_link_libraries(run_app OpenMP::OpenMP_CXX)

    if (ENABLE_JAVA_SDK)
        add_executable(run_java_app test/run_java_app.cc core/java/javasdk.cc core/object/dynamic.cc)
        target_include_directories(run_java_app PRIVATE core utils apps)
        target_compile_definitions(run_java_app PUBLIC ENABLE_JAVA_SDK)
        target_link_libraries(run_java_app gs_proto ${GFLAGS_LIBRARIES} ${MPI_CXX_LIBRARIES} ${JNI_LIBRARIES} ${Boost_LIBRARIES}
                                       ${CMAKE_DL_LIBS} ${VINEYARD_LIBRARIES})

        #java app benchmark
        add_executable(property_graph_java_app_benchmarks benchmarks/property_graph_java_app_benchmarks.cc core/java/javasdk.cc core/object/dynamic.cc)
        target_include_directories(property_graph_java_app_benchmarks PRIVATE core utils apps)
        target_compile_definitions(property_graph_java_app_benchmarks PUBLIC ENABLE_JAVA_SDK)
        target_link_libraries(property_graph_java_app_benchmarks gs_proto ${JNI_LIBRARIES} ${Boost_LIBRARIES} ${CMAKE_DL_LIBS}  ${VINEYARD_LIBRARIES})
        
        # giraph runner
        add_executable(giraph_runner test/giraph_runner.cc core/java/javasdk.cc)
        target_include_directories(giraph_runner PRIVATE core utils apps)
        target_compile_definitions(giraph_runner PUBLIC ENABLE_JAVA_SDK)
        target_link_libraries(giraph_runner ${CMAKE_DL_LIBS} gs_proto ${VINEYARD_LIBRARIES} ${Boost_LIBRARIES} ${GFLAGS_LIBRARIES}  ${JNI_LIBRARIES})

        if (${LIBUNWIND_FOUND})
            target_link_libraries(run_java_app ${LIBUNWIND_LIBRARIES})
            target_link_libraries(property_graph_java_app_benchmarks ${LIBUNWIND_LIBRARIES})
            target_link_libraries(giraph_runner ${LIBUNWIND_LIBRARIES})
        endif ()
    endif()

    macro(add_vineyard_app target)
        set(options)
        set(oneValueArgs)
        set(multiValueArgs SRCS)
        cmake_parse_arguments(add_vineyard_app "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
        add_executable(${target} ${add_vineyard_app_SRCS} core/object/dynamic.cc)
        target_include_directories(${target} PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)
        target_link_libraries(${target} gs_proto ${LIBGRAPELITE_LIBRARIES} ${VINEYARD_LIBRARIES})
        if (${LIBUNWIND_FOUND})
            target_link_libraries(${target} ${LIBUNWIND_LIBRARIES})
        endif ()
        target_link_libraries(${target} OpenMP::OpenMP_CXX)
    endmacro()

    add_vineyard_app(run_vy_app SRCS test/run_vy_app.cc)

    add_vineyard_app(run_load_from_stream SRCS test/run_load_from_stream.cc)

    add_vineyard_app(run_vy_ldbc SRCS test/run_vy_ldbc.cc)
    target_include_directories(run_vy_ldbc PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)

    add_vineyard_app(run_ctx SRCS test/run_ctx.cc)
    target_include_directories(run_ctx PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)
    target_link_libraries(run_ctx gs_proto)

    add_vineyard_app(run_property_ctx SRCS test/run_property_ctx.cc)
    target_include_directories(run_property_ctx PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)
    target_link_libraries(run_property_ctx gs_proto)

    add_vineyard_app(run_pregel_app SRCS test/run_pregel_app.cc)

    add_vineyard_app(run_string_oid SRCS test/run_string_oid.cc)
    target_include_directories(run_string_oid PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)

    add_vineyard_app(run_empty_property SRCS test/run_empty_property.cc)
    target_include_directories(run_empty_property PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)

    add_vineyard_app(test_project_string SRCS test/test_project_string.cc)

    add_vineyard_app(basic_graph_benchmarks SRCS benchmarks/basic_graph_benchmarks.cc)

    add_vineyard_app(property_graph_loader SRCS benchmarks/property_graph_loader.cc)

    add_vineyard_app(property_graph_benchmarks SRCS benchmarks/property_graph_benchmarks.cc)

    add_vineyard_app(projected_graph_benchmarks SRCS benchmarks/projected_graph_benchmarks.cc)

    if (NETWORKX)
        add_vineyard_app(test_convert SRCS test/test_convert.cc)
    endif ()
endif ()

# Cpplint
file(GLOB_RECURSE FILES_NEED_LINT
        "apps/*.cc"
        "apps/*.h"
        "benchmarks/*.cc"
        "benchmarks/*.h"
        "core/*.cc"
        "core/*.h"
        "frame/*.cc"
        "frame/*.h"
        "test/*.cc"
        "test/*.h")
add_custom_target(gsa_cpplint
    COMMAND python3 ${PROJECT_SOURCE_DIR}/misc/cpplint.py ${FILES_NEED_LINT}
    COMMENT "Running cpplint check."
    VERBATIM
)
add_custom_target(gsa_clformat
    COMMAND clang-format --style=file -i ${FILES_NEED_LINT}
    COMMENT "Running clang-format, using clang-format-8 from https://github.com/muttleyxd/clang-tools-static-binaries/releases"
    VERBATIM)

# Install GAE Java SDK
if(ENABLE_JAVA_SDK)
    set(GAE_JAVA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/java/")
    set(GAE_JAVA_RUNTIME_DIR "${GAE_JAVA_DIR}/grape-runtime/")
    set(GAE_JAVA_RUNTIME_JAR "${GAE_JAVA_RUNTIME_DIR}/target/grape-runtime-${GRAPHSCOPE_ANALYTICAL_VERSION}-shaded.jar")
    add_custom_command(
        OUTPUT "${GAE_JAVA_RUNTIME_DIR}/target/native/libgrape-jni.so"
        COMMAND mvn clean install -DskipTests --quiet
        DEPENDS gs_proto
        WORKING_DIRECTORY ${GAE_JAVA_DIR}
        COMMENT "Building GAE-java..."
        VERBATIM
    )
    add_custom_target(grape_jni ALL
        DEPENDS "${GAE_JAVA_RUNTIME_DIR}/target/native/libgrape-jni.so"
    )
    install(FILES DESTINATION lib)
    install(FILES "${GAE_JAVA_RUNTIME_DIR}/target/native/libgrape-jni.so" DESTINATION lib)
    install(FILES "${GAE_JAVA_RUNTIME_JAR}" DESTINATION lib)
    install(FILES "${GAE_JAVA_DIR}/grape_jvm_opts" DESTINATION conf)
endif()

# Install binaries
macro(install_gsa_binary target)
    install(TARGETS ${target}
            EXPORT graphscope-analytical-targets
            ARCHIVE DESTINATION lib
            LIBRARY DESTINATION lib
            RUNTIME DESTINATION bin
    )
endmacro()

# Install headers
macro(install_gsa_headers header_target)
    get_filename_component(ABS_DIR ${header_target} REALPATH)
    install(DIRECTORY ${ABS_DIR}
            DESTINATION include/graphscope      # target directory
            FILES_MATCHING                      # install only matched files
            PATTERN "*.h"                       # select header files
            PATTERN "*.hpp"                     # select C++ template header files
            PATTERN "*javasdk.cc"               # javasdk.cc is needed to compile graph frame
            PATTERN "*dynamic.cc"               # dynamic.cc is needed to compile graph frame
            )
endmacro()

# Install app frames
macro(install_gsa_app_frames source_target)
    get_filename_component(ABS_DIR ${source_target} REALPATH)
    install(DIRECTORY ${ABS_DIR}
            DESTINATION include/graphscope      # target directory
            FILES_MATCHING                      # install only matched files
            PATTERN "*.h"                       # select app frame files
            PATTERN "*.cc"                      # select app frame files
            )
endmacro()

# Install dependencies FindXXX.cmake
macro(install_gsa_dependency_modules cmake_target)
    get_filename_component(ABS_DIR ${cmake_target} REALPATH)
    install(DIRECTORY ${ABS_DIR}
            DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/graphscope-analytical  # target directory
            FILES_MATCHING                                                   # install only matched files
            PATTERN "*.cmake"                                                # select cmake files
            )
endmacro()

install_gsa_binary(grape_engine)
install_gsa_binary(gs_proto)
install_gsa_binary(gs_util)

install_gsa_headers("${PROJECT_SOURCE_DIR}/apps")
install_gsa_headers("${PROJECT_SOURCE_DIR}/benchmarks")
install_gsa_headers("${PROJECT_SOURCE_DIR}/core")
install_gsa_headers("${PROJECT_SOURCE_DIR}/graphscope")
install_gsa_app_frames("${PROJECT_SOURCE_DIR}/frame")

install_gsa_dependency_modules("${PROJECT_SOURCE_DIR}/cmake")

configure_file(graphscope-analytical-config.in.cmake
        "${PROJECT_BINARY_DIR}/graphscope-analytical-config.cmake" @ONLY
        )
configure_file(graphscope-analytical-config-version.in.cmake
        "${PROJECT_BINARY_DIR}/graphscope-analytical-config-version.cmake" @ONLY
        )
install(FILES "${PROJECT_BINARY_DIR}/graphscope-analytical-config.cmake"
        "${PROJECT_BINARY_DIR}/graphscope-analytical-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/graphscope-analytical
        )
install(EXPORT graphscope-analytical-targets
        FILE graphscope-analytical-targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/graphscope-analytical
        )
