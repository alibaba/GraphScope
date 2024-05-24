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

#ifndef ENGINES_HQPS_DB_APP_INTERACTIVE_APP_BASE_H_
#define ENGINES_HQPS_DB_APP_INTERACTIVE_APP_BASE_H_

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/proto_generated_gie/results.pb.h"
#include "flex/utils/property/types.h"
#include "flex/utils/service_utils.h"
#include "nlohmann/json.hpp"
namespace gs {

class GraphDBSession;
template <size_t I, typename TUPLE_T>
bool deserialize_impl(TUPLE_T& tuple, const nlohmann::json& json) {
  return true;
}

template <size_t I, typename TUPLE_T, typename T, typename... ARGS>
bool deserialize_impl(TUPLE_T& tuple, const nlohmann::json& json) {
  PropertyType type = json[I]["type"].get<PropertyType>();
  if (I == json.size()) {
    LOG(ERROR) << "Arguments size mismatch: " << I << " vs " << json.size()
               << ", reach end of json: " << json;
    return false;
  }
  if constexpr (std::is_same<T, std::string>::value ||
                std::is_same<T, std::string_view>::value) {
    if (type != PropertyType::kString && !type.IsVarchar()) {
      LOG(ERROR) << "Argument type mismatch, expected string, but got: "
                 << type;
      return false;
    }
  } else if constexpr (std::is_same<T, int32_t>::value) {
    if (type != PropertyType::kInt32) {
      LOG(ERROR) << "Argument type mismatch, expected int32, but got: " << type;
      return false;
    }
  } else if constexpr (std::is_same<T, int64_t>::value) {
    if (type != PropertyType::kInt64) {
      LOG(ERROR) << "Argument type mismatch, expected int64, but got: " << type;
      return false;
    }
  } else if constexpr (std::is_same<T, uint32_t>::value) {
    if (type != PropertyType::kUInt32) {
      LOG(ERROR) << "Argument type mismatch, expected uint32, but got: "
                 << type;
      return false;
    }
  } else if constexpr (std::is_same<T, uint64_t>::value) {
    if (type != PropertyType::kUInt64) {
      LOG(ERROR) << "Argument type mismatch, expected uint64, but got: "
                 << type;
      return false;
    }
  } else if constexpr (std::is_same<T, double>::value) {
    if (type != PropertyType::kDouble) {
      LOG(ERROR) << "Argument type mismatch, expected double, but got: "
                 << type;
      return false;
    }
  } else if constexpr (std::is_same<T, float>::value) {
    if (type != PropertyType::kFloat) {
      LOG(ERROR) << "Argument type mismatch, expected float, but got: " << type;
      return false;
    }
  } else if constexpr (std::is_same<T, Date>::value) {
    if (type != PropertyType::kDate) {
      LOG(ERROR) << "Argument type mismatch, expected date, but got: " << type;
      return false;
    }
  } else if constexpr (std::is_same<T, Day>::value) {
    if (type != PropertyType::kDay) {
      LOG(ERROR) << "Argument type mismatch, expected day, but got: " << type;
      return false;
    }
  } else {
    LOG(ERROR) << "Unsupported argument type";
    return false;
  }
  if (json[I].contains("value")) {
    std::get<I>(tuple) = json[I]["value"].get<T>();
  } else {
    LOG(ERROR) << "No value found in input";
    return false;
  }
  return deserialize_impl<I + 1, TUPLE_T, ARGS...>(tuple, json);
}

template <typename... ARGS>
bool deserialize(std::tuple<ARGS...>& tuple, std::string_view sv) {
  auto j = nlohmann::json::parse(sv);
  if (!j.contains("arguments")) {
    LOG(ERROR) << "No arguments found in input";
    return false;
  }
  auto arguments_list = j["arguments"];
  if (arguments_list.is_array()) {
    if (arguments_list.size() != sizeof...(ARGS)) {
      LOG(ERROR) << "Arguments size mismatch: " << arguments_list.size()
                 << " vs " << sizeof...(ARGS);
      return false;
    }
    if (arguments_list.size() == 0) {
      VLOG(10) << "No arguments found in input";
      return true;
    }
    return deserialize_impl<0, std::tuple<ARGS...>, ARGS...>(tuple,
                                                             arguments_list);
  } else {
    LOG(ERROR) << "Arguments should be an array";
    return false;
  }
}

// for cypher procedure
template <typename... ARGS>
class CypherReadAppBase : public ReadAppBase {
 public:
  AppType type() const override { return AppType::kCypherProcedure; }

  virtual results::CollectiveResults Query(const GraphDBSession& db,
                                           ARGS... args) = 0;

  bool Query(const GraphDBSession& db, Decoder& input,
             Encoder& output) override {
    auto sv = input.get_string();

    std::tuple<ARGS...> tuple;
    if (!deserialize<ARGS...>(tuple, sv)) {
      LOG(ERROR) << "Failed to deserialize arguments";
      return false;
    }

    // unpack tuple
    auto res = unpackedAndInvoke(db, tuple);
    // write output
    std::string out;
    res.SerializeToString(&out);

    output.put_string(out);
    return true;
  }

  results::CollectiveResults unpackedAndInvoke(const GraphDBSession& db,
                                               std::tuple<ARGS...>& tuple) {
    return std::apply(
        [this, &db](ARGS... args) { return this->Query(db, args...); }, tuple);
  }
};

template <typename... ARGS>
class CypherWriteAppBase : public WriteAppBase {
 public:
  AppType type() const override { return AppType::kCypherProcedure; }

  virtual results::CollectiveResults Query(GraphDBSession& db,
                                           ARGS... args) = 0;

  bool Query(GraphDBSession& db, Decoder& input, Encoder& output) override {
    auto sv = input.get_string();

    std::tuple<ARGS...> tuple;
    if (!deserialize<ARGS...>(tuple, sv)) {
      LOG(ERROR) << "Failed to deserialize arguments";
      return false;
    }

    // unpack tuple
    auto res = unpackedAndInvoke(db, tuple);
    // write output
    std::string out;
    res.SerializeToString(&out);

    output.put_string(out);
    return true;
  }

  results::CollectiveResults unpackedAndInvoke(GraphDBSession& db,
                                               std::tuple<ARGS...>& tuple) {
    return std::apply([this, &db](ARGS... args) { this->Query(db, args...); },
                      tuple);
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_DB_APP_INTERACTIVE_APP_BASE_H_