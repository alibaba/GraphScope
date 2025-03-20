# Copyright 2020-2023 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This File is copied from https://github.com/v6d-io/v6d/blob/main/cmake/BuildEtcdCpp.cmake
# build cpprestsdk
set(WERROR OFF CACHE BOOL "Treat warnings as errors")
set(BUILD_TESTS OFF CACHE BOOL "Build tests.")
set(BUILD_SAMPLES OFF CACHE BOOL "Build sample applications.")
set(CPPREST_EXCLUDE_WEBSOCKETS ON CACHE BOOL "Exclude websockets functionality..")
add_subdirectory(third_party/cpprestsdk)
set(CPPREST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party/cpprestsdk/Release/include)
set(CPPREST_LIB cpprest)

# disable a warning message inside cpprestsdk on Mac with llvm/clang
if(W_NO_UNUSED_BUT_SET_PARAMETER)
    target_compile_options(cpprest PRIVATE -Wno-unused-but-set-parameter)
endif()

# build etcd-cpp-apiv3
add_subdirectory(third_party/etcd-cpp-apiv3)
set(ETCD_CPP_LIBRARIES etcd-cpp-api)
set(ETCD_CPP_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party/etcd-cpp-apiv3/
                         ${PROJECT_BINARY_DIR}/third_party/etcd-cpp-apiv3/proto/gen
                         ${PROJECT_BINARY_DIR}/third_party/etcd-cpp-apiv3/proto/gen/proto)
