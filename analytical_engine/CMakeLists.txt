cmake_minimum_required(VERSION 3.1)

if ("${GRAPHSCOPE_VERSION}" STREQUAL "")
    set(GRAPHSCOPE_ANALYTICAL_MAJOR_VERSION 0)
    set(GRAPHSCOPE_ANALYTICAL_MINOR_VERSION 6)
    set(GRAPHSCOPE_ANALYTICAL_PATCH_VERSION 0)
    set(GRAPHSCOPE_ANALYTICAL_VERSION ${GRAPHSCOPE_ANALYTICAL_MAJOR_VERSION}.${GRAPHSCOPE_ANALYTICAL_MINOR_VERSION}.${GRAPHSCOPE_ANALYTICAL_PATCH_VERSION})
else ()
    set(GRAPHSCOPE_ANALYTICAL_MAJOR_VERSION ${GRAPHSCOPE_MAJOR_VERSION})
    set(GRAPHSCOPE_ANALYTICAL_MINOR_VERSION ${GRAPHSCOPE_MINOR_VERSION})
    set(GRAPHSCOPE_ANALYTICAL_PATCH_VERSION ${GRAPHSCOPE_PATCH_VERSION})
    set(GRAPHSCOPE_ANALYTICAL_VERSION ${GRAPHSCOPE_VERSION})
endif ()

if (POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif ()
project(analytical_engine LANGUAGES C CXX VERSION ${GRAPHSCOPE_ANALYTICAL_VERSION})

option(NETWORKX "networkx on?" ON)
option(BUILD_TESTS "Build unit test" ON)

if (NETWORKX)
    add_definitions(-DNETWORKX)
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

include(CheckCXXCompilerFlag)
include(CheckLibraryExists)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include_directories(${PROJECT_SOURCE_DIR})

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
if(W_NO_CLASS_MEMACCESS)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess")
endif()
if(W_NO_REDUNDANT_MOVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-redundant-move")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
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

# Find dependencies
include("cmake/FindRdkafka.cmake")
if (RDKAFKA_FOUND)
    include_directories(SYSTEM ${RDKAFKA_INCLUDE_DIRS})
endif ()

find_package(Threads REQUIRED)
find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

find_package(Boost REQUIRED COMPONENTS system filesystem
             # required by folly
             context program_options regex thread)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
# eliminate a lot of warnings for newer version of boost library.
add_compile_options(-DBOOST_BIND_GLOBAL_PLACEHOLDERS)

include("cmake/FindGFlags.cmake")
if (GFLAGS_FOUND)
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "gflags not found")
endif ()

include("cmake/FindGlog.cmake")
include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})

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

find_package(vineyard 0.2.6 REQUIRED)
include_directories(${VINEYARD_INCLUDE_DIRS})
add_compile_options(-DENABLE_SELECTOR)

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

include("cmake/FindGRPC.cmake")
include_directories(${GRPC_INCLUDE_DIR})

if (NETWORKX)
    include("cmake/FindFolly.cmake")
    include("cmake/FindDoubleConversion.cmake")
    if (FOLLY_FOUND)
        include_directories(${FOLLY_ROOT_DIR}/include)
    else ()
        message(FATAL_ERROR "folly not found")
    endif ()
else ()
    set(FOLLY_LIBRARIES "")
    set(DOUBLE_CONVERSION_LIBRARY "")
endif ()

# Generate proto
execute_process(COMMAND python3 proto/proto_generator.py "${PROJECT_SOURCE_DIR}" --cpp
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/..)
execute_process(COMMAND python3 proto/proto_generator.py "${PROJECT_SOURCE_DIR}/../python/graphscope" --python
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/..)

file(GLOB PROTO_CPP_FILES "proto/*.cc")
file(GLOB CORE_GS_FILES "core/*.cc")
file(GLOB CORE_RPC_SRC_FILES "core/server/*.cc")

# Add targets
add_library(gs_proto SHARED ${PROTO_CPP_FILES})
set_source_files_properties(${PROTO_CPP_FILES} PROPERTIES COMPILE_FLAGS -Wno-array-bounds)
target_include_directories(gs_proto PRIVATE "${PROJECT_SOURCE_DIR}/proto")
target_link_libraries(gs_proto ${Protobuf_LIBRARIES} ${GRPC_LIBRARY}
    ${GRPC_GRPC++_LIBRARY} ${GRPC_GRPC++_REFLECTION_LIBRARY})

add_executable(grape_engine ${CORE_GS_FILES} ${CORE_RPC_SRC_FILES})
target_include_directories(grape_engine PRIVATE proto utils)
target_link_libraries(grape_engine PRIVATE gs_proto
        ${LIBGRAPELITE_LIBRARIES}
        ${GFLAGS_LIBRARIES}
        ${CMAKE_DL_LIBS}
        ${Boost_LIBRARIES}
        ${VINEYARD_LIBRARIES}
        )

if (${LIBUNWIND_FOUND})
    target_link_libraries(grape_engine PRIVATE ${LIBUNWIND_LIBRARIES})
endif ()

if (NETWORKX)
    target_include_directories(grape_engine PUBLIC ${FOLLY_ROOT_DIR}/include)
    target_link_libraries(grape_engine PRIVATE ${FOLLY_LIBRARIES} ${DOUBLE_CONVERSION_LIBRARY})
endif ()

# Test targets
if (BUILD_TESTS)
    add_executable(run_app test/run_app.cc)
    target_include_directories(run_app PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps utils apps)
    target_link_libraries(run_app ${LIBGRAPELITE_LIBRARIES} ${GFLAGS_LIBRARIES} ${CMAKE_DL_LIBS} ${Boost_LIBRARIES} ${VINEYARD_LIBRARIES})
    if (NETWORKX)
        target_include_directories(run_app PUBLIC ${FOLLY_ROOT_DIR}/include)
        target_link_libraries(run_app ${FOLLY_LIBRARIES} ${DOUBLE_CONVERSION_LIBRARY})
    endif ()

    macro(add_vineyard_app target)
        set(options)
        set(oneValueArgs)
        set(multiValueArgs SRCS)
        cmake_parse_arguments(add_vineyard_app "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
        add_executable(${target} ${add_vineyard_app_SRCS})
        target_include_directories(${target} PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)
        target_link_libraries(${target} ${LIBGRAPELITE_LIBRARIES} ${VINEYARD_LIBRARIES})
        if (${LIBUNWIND_FOUND})
            target_link_libraries(${target} ${LIBUNWIND_LIBRARIES})
        endif ()
    endmacro()

    add_vineyard_app(run_vy_app SRCS test/run_vy_app.cc)

    add_vineyard_app(run_load_from_stream SRCS test/run_load_from_stream.cc)

    add_vineyard_app(run_vy_ldbc SRCS test/run_vy_ldbc.cc)
    target_include_directories(run_vy_ldbc PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)

    if (RDKAFKA_FOUND)
        add_vineyard_app(run_append_frag SRCS test/run_append_frag.cc)
        target_include_directories(run_append_frag PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/examples/gnn_sampler)
        target_link_libraries(run_append_frag ${GFLAGS_LIBRARIES} ${RDKAFKA_LIBRARIES})
    endif ()

    add_vineyard_app(run_ctx SRCS test/run_ctx.cc)
    target_include_directories(run_ctx PRIVATE ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)
    target_link_libraries(run_ctx gs_proto)

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
        target_include_directories(test_convert PRIVATE ${FOLLY_ROOT_DIR}/include)
        target_link_libraries(test_convert ${FOLLY_LIBRARIES} ${DOUBLE_CONVERSION_LIBRARY})
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
            DESTINATION lib                     # target directory
            FILES_MATCHING                      # install only matched files
            PATTERN "*.cmake"                   # select cmake files
            )
endmacro()

install_gsa_binary(grape_engine)
install_gsa_binary(gs_proto)

install_gsa_headers("${PROJECT_SOURCE_DIR}/apps")
install_gsa_headers("${PROJECT_SOURCE_DIR}/benchmarks")
install_gsa_headers("${PROJECT_SOURCE_DIR}/core")
install_gsa_headers("${PROJECT_SOURCE_DIR}/proto")
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
