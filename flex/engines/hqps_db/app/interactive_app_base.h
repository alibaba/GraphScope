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
#include "flex/proto_generated_gie/stored_procedure.pb.h"
#include "flex/utils/property/types.h"
#include "flex/utils/service_utils.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
namespace gs {

void put_argument(gs::Encoder& encoder, const procedure::Argument& argument) {
  auto& value = argument.value();
  auto item_case = value.item_case();
  switch (item_case) {
  case common::Value::kI32:
    encoder.put_int(value.i32());
    break;
  case common::Value::kI64:
    encoder.put_long(value.i64());
    break;
  case common::Value::kF64:
    encoder.put_double(value.f64());
    break;
  case common::Value::kStr:
    encoder.put_string(value.str());
    break;
  default:
    LOG(ERROR) << "Not recognizable param type" << static_cast<int>(item_case);
  }
}

bool parse_input_argument(gs::Decoder& raw_input,
                          gs::Encoder& argument_encoder) {
  if (raw_input.size() == 0) {
    VLOG(10) << "No arguments found in input";
    return true;
  }
  procedure::Query cur_query;
  if (!cur_query.ParseFromArray(raw_input.data(), raw_input.size())) {
    LOG(ERROR) << "Fail to parse query from input content";
    return false;
  }
  auto& args = cur_query.arguments();
  for (int32_t i = 0; i < args.size(); ++i) {
    put_argument(argument_encoder, args[i]);
  }
  VLOG(10) << ", num args: " << args.size();
  return true;
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
      std::get<I>(tuple) = json[I]["value"].GetObj();
    }
  } else {
    LOG(ERROR) << "No value found in input";
    return false;
  }
  return deserialize_impl<I + 1, TUPLE_T, ARGS...>(tuple, json);
}

template <typename... ARGS>
bool deserialize(std::tuple<ARGS...>& tuple, std::string_view sv) {
  rapidjson::Document j;
  VLOG(10) << "parsing string: " << sv << ",size" << sv.size();
  if (j.Parse(sv.data()).HasParseError()) {
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

// For internal cypher-gen procedure, with pb input and output
// Codegen app should inherit from this class
class CypherInternalPbWriteAppBase : public WriteAppBase {
 public:
  AppType type() const override { return AppType::kCypherProcedure; }

  virtual bool DoQuery(GraphDBSession& db, Decoder& input, Encoder& output) = 0;

  bool Query(GraphDBSession& db, Decoder& raw_input, Encoder& output) override {
    std::vector<char> output_buffer;
    gs::Encoder argument_encoder(output_buffer);
    if (!parse_input_argument(raw_input, argument_encoder)) {
      LOG(ERROR) << "Failed to parse input argument!";
      return false;
    }
    gs::Decoder argument_decoder(output_buffer.data(), output_buffer.size());
    return DoQuery(db, argument_decoder, output);
  }
};

}  // namespace gs

#endif  // ENGINES_HQPS_DB_APP_INTERACTIVE_APP_BASE_H_
