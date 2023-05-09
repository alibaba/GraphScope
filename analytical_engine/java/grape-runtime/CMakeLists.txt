# Copyright 2021 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.1)
project(GRAPE-JNI)

execute_process(COMMAND uname -m OUTPUT_VARIABLE SYSTEM_PROCESSOR)
string(REGEX REPLACE "\n$" "" SYSTEM_PROCESSOR "${SYSTEM_PROCESSOR}")

if (APPLE)
    if (POLICY CMP0056)
        cmake_policy(SET CMP0056 NEW)
    endif ()
    if (SYSTEM_PROCESSOR MATCHES "arm64")
    	message(STATUS "Building on arch arm64")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L/opt/homebrew/lib")
        set(CMAKE_OSX_ARCHITECTURES "arm64")
        set(CMAKE_HOST_SYSTEM_PROCESSOR "arm64")
    else()
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L/usr/local/lib")
    endif()
endif ()

include(CheckCXXCompilerFlag)

#Check compiler is clang or gnu
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Using Clang compiler: ${CMAKE_CXX_COMPILER}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fforce-emit-vtables")
    message(STATUS "Try to find ld.lld under same directory..")
    get_filename_component(COMPILER_DIR ${CMAKE_CXX_COMPILER} DIRECTORY)
    message(STATUS "Obtain directory: ${COMPILER_DIR}")
    if (NOT APPLE)
        #ld.lld can't work on mac.
        set(CMAKE_JNI_LINKER_FLAGS "-fuse-ld=${COMPILER_DIR}/ld.lld -Xlinker -mllvm=-lto-embed-bitcode")
    endif()
else()
    message(STATUS "Using a non-clang compiler will lose performance evaluation provided by LLVM4JNI. Clang-11 compiler is recommended")
    message(STATUS "Your compiler is ${CMAKE_CXX_COMPILER}")
endif()

if(NOT "${CMAKE_CXX_STANDARD}")
    check_cxx_compiler_flag(-std=c++14 HAVE_FLAG_STD_CXX14)
    check_cxx_compiler_flag(-std=c++17 HAVE_FLAG_STD_CXX17)
    if(HAVE_FLAG_STD_CXX17)
        set(CMAKE_CXX_STANDARD 17)
    elseif(HAVE_FLAG_STD_CXX14)
        set(CMAKE_CXX_STANDARD 14)
    else()
        set(CMAKE_CXX_STANDARD 11)
    endif()
endif()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -flto")

file(GLOB SOURCES "target/generated-sources/annotations/*.cc" "src/main/native/*.cc")

set(JAVA_AWT_LIBRARY NotNeeded)
set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})

find_package(Boost REQUIRED COMPONENTS system filesystem
             # required by folly
             context program_options regex thread)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
# eliminate a lot of warnings for newer version of boost library.
add_compile_options(-DBOOST_BIND_GLOBAL_PLACEHOLDERS)

# find libgrape-lite-------------------------------------------------------------------
find_package(libgrapelite REQUIRED)
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS})

# find vineyard ---------------------------------------------
find_package(vineyard 0.14.5 REQUIRED)
include_directories(${VINEYARD_INCLUDE_DIRS})

add_library(grape-jni SHARED ${SOURCES})
target_compile_definitions(grape-jni PUBLIC ENABLE_JAVA_SDK)
target_include_directories(grape-jni PRIVATE "${PROJECT_SOURCE_DIR}/../../")
target_link_libraries(grape-jni ${CMAKE_JNI_LINKER_FLAGS}  ${LIBGRAPELITE_LIBRARIES} ${Boost_LIBRARIES} ${VINEYARD_LIBRARIES})
set_target_properties(grape-jni PROPERTIES LINKER_LANGUAGE CXX)
target_compile_features(grape-jni PRIVATE "cxx_std_${CMAKE_CXX_STANDARD}")
