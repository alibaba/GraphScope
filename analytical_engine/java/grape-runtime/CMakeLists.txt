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

#Check compiler is clang or gnu
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Using Clang compiler: ${CMAKE_CXX_COMPILER}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -flto -fforce-emit-vtables")
    message(STATUS "Try to find ld.lld under same directory..")
    get_filename_component(COMPILER_DIR ${CMAKE_CXX_COMPILER} DIRECTORY)
    message(STATUS "Obtain directory: ${COMPILER_DIR}")
    set(CMAKE_JNI_LINKER_FLAGS "-fuse-ld=${COMPILER_DIR}/ld.lld -Xlinker -mllvm=-lto-embed-bitcode")
else()
    message(STATUS "Using a non-clang compiler will lose performance evaluation provided by LLVM4JNI. Clang-11 compiler is recommended")
    message(STATUS "Your compiler is ${CMAKE_CXX_COMPILER}")
endif()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -std=gnu++14 -Wall -Werror")

file(GLOB SOURCES "target/generated-sources/annotations/*.cc" "src/main/cpp/*.cc")

set(JAVA_AWT_LIBRARY NotNeeded)
set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})
include_directories("src/main/cpp")

# find libgrape-lite-------------------------------------------------------------------
find_package(libgrapelite REQUIRED)
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS})

# find vineyard ---------------------------------------------
find_package(vineyard 0.2.6 REQUIRED)
include_directories(${VINEYARD_INCLUDE_DIRS})

# find graphscope analytical engine---------------------------------------------
find_package(graphscope-analytical)
include_directories(SYSTEM ${GRAPHSCOPE_ANALYTICAL_INCLUDE_DIRS})

find_package(Threads REQUIRED)

add_library(grape-jni SHARED ${SOURCES})
target_compile_definitions(grape-jni PUBLIC ENABLE_JAVA_SDK)
target_link_libraries(grape-jni ${CMAKE_JNI_LINKER_FLAGS}  ${LIBGRAPELITE_LIBRARIES} ${VINEYARD_LIBRARIES})
set_target_properties(grape-jni PROPERTIES LINKER_LANGUAGE CXX)
target_compile_features(grape-jni PRIVATE cxx_std_14)
