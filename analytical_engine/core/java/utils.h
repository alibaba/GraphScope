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
#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

namespace gs {
#define INSTALL_JAVA_PARALLEL_WORKER(APP_T, CONTEXT_T, FRAG_T)    \
 public:                                                          \
  using fragment_t = FRAG_T;                                      \
  using context_t = CONTEXT_T;                                    \
  using message_manager_t = grape::ParallelMessageManager;        \
  using worker_t = grape::ParallelWorker<APP_T>;                  \
  static std::shared_ptr<worker_t> CreateWorker(                  \
      std::shared_ptr<APP_T> app, std::shared_ptr<FRAG_T> frag) { \
    return std::shared_ptr<worker_t>(new worker_t(app, frag));    \
  }
#define INSTALL_JAVA_PARALLEL_PROPERTY_WORKER(APP_T, CONTEXT_T, FRAG_T) \
 public:                                                                \
  using fragment_t = FRAG_T;                                            \
  using context_t = CONTEXT_T;                                          \
  using message_manager_t = ParallelPropertyMessageManager;             \
  using worker_t = ParallelPropertyWorker<APP_T>;                       \
  static std::shared_ptr<worker_t> CreateWorker(                        \
      std::shared_ptr<APP_T> app, std::shared_ptr<FRAG_T> frag) {       \
    return std::shared_ptr<worker_t>(new worker_t(app, frag));          \
  }
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

static constexpr const char* GRAPHX_PREGEL_TASK = "run_pregel";
static constexpr const char* LOAD_FRAGMENT = "load_fragment";
static constexpr const char* LOAD_FRAGMENT_RES_PREFIX =
    "ArrowProjectedFragmentID";

using ptree = boost::property_tree::ptree;
static inline void string2ptree(const std::string& params, ptree& pt) {
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
template <typename T>
struct TypeName {
  static std::string Get() { return "std::string"; }
};

// a specialization for each type of those you want to support
// and don't like the string returned by typeid
template <>
struct TypeName<int32_t> {
  static std::string Get() { return "int32_t"; }
};
template <>
struct TypeName<int64_t> {
  static std::string Get() { return "int64_t"; }
};
template <>
struct TypeName<double> {
  static std::string Get() { return "double"; }
};
template <>
struct TypeName<uint32_t> {
  static std::string Get() { return "uint32_t"; }
};
template <>
struct TypeName<uint64_t> {
  static std::string Get() { return "uint64_t"; }
};

template <typename T>
std::shared_ptr<vineyard::Object> buildPrimitiveArray(
    vineyard::Client& client, std::vector<T>& raw_data) {
  using arrow_builder_t = typename vineyard::ConvertToArrowType<T>::BuilderType;
  arrow_builder_t builder;
  ARROW_CHECK_OK(builder.AppendValues(raw_data));

  using arrow_array_t = typename vineyard::ConvertToArrowType<T>::ArrayType;
  std::shared_ptr<arrow_array_t> arrow_array;
  ARROW_CHECK_OK(builder.Finish(&arrow_array));

  using vineyard_builder_t =
      typename vineyard::ConvertToArrowType<T>::VineyardBuilderType;
  vineyard_builder_t v6d_builder(client, arrow_array);

  return v6d_builder.Seal(client);
}

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_UTILS_H_
