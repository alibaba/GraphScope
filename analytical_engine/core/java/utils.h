/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ANALYTICAL_ENGINE_CORE_JAVA_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_UTILS_H_

#ifdef ENABLE_JAVA_SDK

#include <jni.h>
#include <string>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

namespace gs {

// data vector contains all bytes, can be used to hold oid and vdata, edata.
using byte_vector = std::vector<char>;
// offset vector contains offsets to deserialize data vector.
using offset_vector = std::vector<int>;

static constexpr const char* OFFSET_VECTOR_VECTOR =
    "std::vector<std::vector<int>>";
static constexpr const char* DATA_VECTOR_VECTOR =
    "std::vector<std::vector<char>>";

static constexpr const char* GIRAPH_PARAMS_CHECK_CLASS =
    "org/apache/giraph/utils/GiraphParamsChecker";
static constexpr const char* VERIFY_CLASSES_SIGN =
    "(Ljava/lang/String;Ljava/lang/String;)V";

static constexpr const char* OPTION_LOADING_THREAD_NUM = "loading_thread_num";
static constexpr const char* OPTION_VERTEX_INPUT_FORMAT_CLASS =
    "vertex_input_format_class";
static constexpr const char* OPTION_EDGE_INPUT_FORMAT_CLASS =
    "edge_input_format_class";
static constexpr const char* OPTION_VERTEX_OUTPUT_FORMAT_CLASS =
    "vertex_output_format_class";
static constexpr const char* OPTION_EFILE = "efile";
static constexpr const char* OPTION_VFILE = "vfile";
static constexpr const char* OPTION_QUERY_TIMES = "query_times";
static constexpr const char* OPTION_SERIALIZE = "serialize";
static constexpr const char* OPTION_DESERIALIZE = "deserialize";
static constexpr const char* OPTION_SERIALIZE_PREFIX = "serialize_prefix";
static constexpr const char* OPTION_USER_APP_CLASS = "user_app_class";
static constexpr const char* OPTION_DRIVER_APP_CLASS = "java_driver_app";
static constexpr const char* OPTION_DRIVER_CONTEXT_CLASS =
    "java_driver_context";
static constexpr const char* OPTION_LIB_PATH = "lib_path";
static constexpr const char* OPTION_GRAPE_LOADER = "grape_loader";
static constexpr const char* OPTION_DIRECTED = "directed";
static constexpr const char* OPTION_IPC_SOCKET = "ipc_socket";
static constexpr const char* OPTION_FRAG_IDS = "frag_ids";

using ptree = boost::property_tree::ptree;

void string2ptree(const std::string& params, ptree& pt) {
  std::stringstream ss;
  {
    ss << params;
    try {
      boost::property_tree::read_json(ss, pt);
    } catch (boost::property_tree::ptree_error& r) {
      LOG(ERROR) << "Parsing json failed: " << params;
    }
  }
}

template <typename T>
static T getFromPtree(const ptree& pt, const char* key) {
  return pt.get<T>(key);
}

}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_UTILS_H_
