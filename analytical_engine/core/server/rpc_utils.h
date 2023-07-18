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
#ifndef ANALYTICAL_ENGINE_CORE_SERVER_RPC_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_SERVER_RPC_UTILS_H_

#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "boost/leaf/result.hpp"
#include "google/protobuf/util/json_util.h"
#include "vineyard/graph/utils/error.h"

#include "core/config.h"
#include "core/server/command_detail.h"
#include "proto/attr_value.pb.h"
#include "proto/graph_def.pb.h"
#include "proto/op_def.pb.h"
#include "proto/types.pb.h"

namespace bl = boost::leaf;

namespace gs {
namespace rpc {

template <typename T>
inline T get_param_impl(const std::map<int, rpc::AttrValue>& params,
                        rpc::ParamKey key) {
  __builtin_unreachable();
}

template <>
inline std::string get_param_impl<std::string>(
    const std::map<int, rpc::AttrValue>& params, rpc::ParamKey key) {
  return params.at(key).s();
}

template <>
inline int64_t get_param_impl<int64_t>(
    const std::map<int, rpc::AttrValue>& params, rpc::ParamKey key) {
  return params.at(key).i();
}

template <>
inline uint64_t get_param_impl<uint64_t>(
    const std::map<int, rpc::AttrValue>& params, rpc::ParamKey key) {
  return params.at(key).u();
}

template <>
inline bool get_param_impl<bool>(const std::map<int, rpc::AttrValue>& params,
                                 rpc::ParamKey key) {
  return params.at(key).b();
}

template <>
inline float get_param_impl<float>(const std::map<int, rpc::AttrValue>& params,
                                   rpc::ParamKey key) {
  return params.at(key).f();
}

template <>
inline rpc::graph::GraphTypePb get_param_impl<rpc::graph::GraphTypePb>(
    const std::map<int, rpc::AttrValue>& params, rpc::ParamKey key) {
  return static_cast<rpc::graph::GraphTypePb>(params.at(key).i());
}

template <>
inline rpc::ModifyType get_param_impl<rpc::ModifyType>(
    const std::map<int, rpc::AttrValue>& params, rpc::ParamKey key) {
  return static_cast<rpc::ModifyType>(params.at(key).i());
}

template <>
inline rpc::AttrValue_ListValue get_param_impl<rpc::AttrValue_ListValue>(
    const std::map<int, rpc::AttrValue>& params, rpc::ParamKey key) {
  return params.at(key).list();
}

template <>
inline rpc::ReportType get_param_impl<rpc::ReportType>(
    const std::map<int, rpc::AttrValue>& params, rpc::ParamKey key) {
  return static_cast<rpc::ReportType>(params.at(key).i());
}

/**
 * @brief GSParams is a wrapper class for rpc::AttrValue. This class provides
 * useful methods to get and check parameters from the coordinator.
 */
class GSParams {
 public:
  explicit GSParams(std::map<int, rpc::AttrValue> params,
                    const rpc::LargeAttrValue& large_attr)
      : params_(std::move(params)), large_attr_(large_attr) {}

  template <typename T>
  bl::result<T> Get(rpc::ParamKey key) const {
    if (params_.find(key) == params_.end()) {
      RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                      "Can not found key: " + rpc::ParamKey_Name(key));
    }
    return get_param_impl<T>(params_, key);
  }

  template <typename T>
  bl::result<T> Get(rpc::ParamKey key, T const& default_value) const {
    if (params_.find(key) == params_.end()) {
      return default_value;
    }
    return get_param_impl<T>(params_, key);
  }

  bool HasKey(rpc::ParamKey key) const {
    return params_.find(key) != params_.end();
  }

  const rpc::LargeAttrValue& GetLargeAttr() const { return large_attr_; }

  const std::string DebugString() const {
    std::ostringstream ss;
    ss << "GSParams: {";
    for (auto const& kv : params_) {
      ss << rpc::ParamKey_Name(static_cast<rpc::ParamKey>(kv.first)) << ": "
         << kv.second.DebugString() << ", ";
    }
    ss << "}";
    return ss.str();
  }

 private:
  const std::map<int, rpc::AttrValue> params_;
  const rpc::LargeAttrValue& large_attr_;
};

inline bl::result<DagDef> ReadDagFromFile(const std::string& location) {
  std::ifstream ifs(location);
  std::string dag_str((std::istreambuf_iterator<char>(ifs)),
                      (std::istreambuf_iterator<char>()));

  DagDef dag_def;
  auto stat = google::protobuf::util::JsonStringToMessage(dag_str, &dag_def);
  if (!stat.ok()) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kIOError,
                    "Failed to parse: " + stat.ToString());
  }
  return dag_def;
}

inline std::shared_ptr<CommandDetail> OpToCmd(const OpDef& op) {
  auto op_type = op.op();
  std::map<int, rpc::AttrValue> params;

  for (auto& pair : op.attr()) {
    params[pair.first] = pair.second;
  }
  // large attr
  return op.has_query_args()
             ? std::make_shared<CommandDetail>(op_type, std::move(params),
                                               op.large_attr(), op.query_args())
             : std::make_shared<CommandDetail>(op_type, std::move(params),
                                               op.large_attr());
}
}  // namespace rpc
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_SERVER_RPC_UTILS_H_
