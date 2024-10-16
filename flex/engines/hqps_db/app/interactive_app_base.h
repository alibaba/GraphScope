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

#include <rapidjson/document.h>
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/proto_generated_gie/results.pb.h"
#include "flex/proto_generated_gie/stored_procedure.pb.h"
#include "flex/utils/property/types.h"
#include "flex/utils/service_utils.h"

namespace gs {

template <size_t I, typename TUPLE_T, typename... ARGS>
inline bool parse_input_argument_from_proto_impl(
    TUPLE_T& tuple,
    const google::protobuf::RepeatedPtrField<procedure::Argument>& args) {
  if constexpr (I == sizeof...(ARGS)) {
    return true;
  } else {
    auto& type = std::get<I>(tuple);
    auto& argument = args.Get(I);
    if (argument.value_case() != procedure::Argument::kConst) {
      LOG(ERROR) << "Expect a const value for input param, but got "
                 << argument.value_case();
      return false;
    }
    auto& value = argument.const_();
    auto item_case = value.item_case();
    if (item_case == common::Value::kI32) {
      if constexpr (std::is_same<int32_t,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.i32();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else if (item_case == common::Value::kI64) {
      if constexpr (std::is_same<int64_t,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.i64();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else if (item_case == common::Value::kF64) {
      if constexpr (std::is_same<double,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.f64();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else if (item_case == common::Value::kStr) {
      if constexpr (std::is_same<std::string,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.str();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else {
      LOG(ERROR) << "Not recognizable param type" << item_case;
      return false;
    }
    return parse_input_argument_from_proto_impl<I + 1, TUPLE_T, ARGS...>(tuple,
                                                                         args);
  }
}

template <typename... ARGS>
inline bool parse_input_argument_from_proto(std::tuple<ARGS...>& tuple,
                                            std::string_view sv) {
  if (sv.size() == 0) {
    VLOG(10) << "No arguments found in input";
    return true;
  }
  procedure::Query cur_query;
  if (!cur_query.ParseFromArray(sv.data(), sv.size())) {
    LOG(ERROR) << "Fail to parse query from input content";
    return false;
  }
  auto& args = cur_query.arguments();
  if (args.size() != sizeof...(ARGS)) {
    LOG(ERROR) << "Arguments size mismatch: " << args.size() << " vs "
               << sizeof...(ARGS);
    return false;
  }
  return parse_input_argument_from_proto_impl<0, std::tuple<ARGS...>, ARGS...>(
      tuple, args);
}

class GraphDBSession;

template <size_t I, typename TUPLE_T>
bool deserialize_impl(TUPLE_T& tuple, const rapidjson::Value& json) {
  return true;
}

template <size_t I, typename TUPLE_T, typename T, typename... ARGS>
bool deserialize_impl(TUPLE_T& tuple, const rapidjson::Value& json) {
  if (I == json.Size()) {
    LOG(ERROR) << "Arguments size mismatch: " << I << " vs " << json.Size()
               << ", reach end of json: " << rapidjson_stringify(json);
    return false;
  }
  auto& type_json = json[I]["type"];
  PropertyType type;
  from_json(type_json, type);
  if (type == PropertyType::Empty()) {
    LOG(ERROR) << "Fail to parse type from input content";
    return false;
  }

  auto expected_type = AnyConverter<T>::type();
  if (type != expected_type) {
    LOG(ERROR) << "Type mismatch: " << type << " vs " << expected_type;
    return false;
  }

  if (json[I].HasMember("value")) {
    if constexpr (std::is_same<T, gs::Date>::value) {
      std::get<I>(tuple).milli_second = json[I]["value"].GetInt64();
    } else if constexpr (std::is_same<T, gs::Day>::value) {
      std::get<I>(tuple).day = json[I]["value"].GetUint();
    } else {
      std::get<I>(tuple) = json[I]["value"].Get<T>();
    }
  } else {
    LOG(ERROR) << "No value found in input";
    return false;
  }
  return deserialize_impl<I + 1, TUPLE_T, ARGS...>(tuple, json);
}

template <typename... ARGS>
bool parse_input_argument_from_json(std::tuple<ARGS...>& tuple,
                                    std::string_view sv) {
  rapidjson::Document j;
  VLOG(10) << "parsing string: " << sv << ",size" << sv.size();
  if (sv.empty()) {
    LOG(INFO) << "No arguments found in input";
    return sizeof...(ARGS) == 0;
  }
  if (j.Parse(std::string(sv)).HasParseError()) {
    LOG(ERROR) << "Fail to parse json from input content";
    return false;
  }
  if (!j.HasMember("arguments")) {
    LOG(INFO) << "No arguments found in input";
    return sizeof...(ARGS) == 0;
  }
  auto& arguments_list = j["arguments"];
  if (arguments_list.IsArray()) {
    if (arguments_list.Size() != sizeof...(ARGS)) {
      LOG(ERROR) << "Arguments size mismatch: " << arguments_list.Size()
                 << " vs " << sizeof...(ARGS);
      return false;
    }
    if (arguments_list.Size() == 0) {
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

template <typename... ARGS>
bool deserialize(std::tuple<ARGS...>& tuple, std::string_view sv) {
  // Deserialize input argument from the payload. The last byte is the input
  // format, could only be kCypherJson or kCypherProtoProcedure.
  if (sv.empty()) {
    return sizeof...(ARGS) == 0;
  }
  auto input_format = static_cast<uint8_t>(sv.back());
  std::string_view payload(sv.data(), sv.size() - 1);
  if (input_format ==
      static_cast<uint8_t>(GraphDBSession::InputFormat::kCypherJson)) {
    return parse_input_argument_from_json(tuple, payload);
  } else if (input_format ==
             static_cast<uint8_t>(
                 GraphDBSession::InputFormat::kCypherProtoProcedure)) {
    return parse_input_argument_from_proto(tuple, payload);
  } else {
    LOG(ERROR) << "Invalid input format: " << input_format;
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
    std::string_view sv(input.data(), input.size());

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
    std::string_view sv(input.data(), input.size());

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
