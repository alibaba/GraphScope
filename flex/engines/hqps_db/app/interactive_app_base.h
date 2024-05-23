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
#include "nlohmann/json.hpp"
namespace gs {

class GraphDBSession;
template <size_t I>
std::tuple<> deserialize_impl(const nlohmann::json& sv) {
  return std::make_tuple();
}

template <size_t I, typename T, typename... ARGS>
std::tuple<T, ARGS...> deserialize_impl(const nlohmann::json& arguments_list) {
  T value{};
  PropertyType type{};
  from_json(arguments_list[I]["type"], type);
  if constexpr (std::is_same<T, std::string>::value ||
                std::is_same<T, std::string_view>::value) {
    if (type != PropertyType::kString && !type.IsVarchar()) {
      throw std::runtime_error("Argument type mismatch");
    }
    value = arguments_list[I]["value"].get<std::string>();
  } else if constexpr (std::is_same<T, gs::Date>::value) {
    if (type != PropertyType::kDate) {
      throw std::runtime_error("Argument type mismatch");
    }
    value.milli_second = gs::Date{arguments_list[I]["value"].get<int64_t>()};
  } else if constexpr (std::is_same<T, gs::Day>::value) {
    if (type != PropertyType::kDay) {
      throw std::runtime_error("Argument type mismatch");
    }
    value.day = gs::Day{arguments_list[I]["value"].get<uint32_t>()};
  } else {
    if (type != AnyConverter<T>::value) {
      throw std::runtime_error("Argument type mismatch");
    }
    value = arguments_list[I]["value"].get<T>();
  }
  return std::tuple_cat(std::make_tuple(value),
                        deserialize_impl<I + 1, ARGS...>(arguments_list));
}

template <typename... ARGS>
std::tuple<ARGS...> deserialize(std::string_view sv) {
  auto j = nlohmann::json::parse(sv);
  auto arguments_list = j["arguments"];
  return deserialize_impl<0, ARGS...>(arguments_list);
}

class ReadAppBase : public AppBase {
 public:
  AppMode mode() const override { return AppMode::kRead; }

  AppType type() const override { return AppType::kCppProcedure; }

  bool run(GraphDBSession& db, Decoder& input, Encoder& output) override {
    return this->Query(db, input, output);
  }

  virtual bool Query(const GraphDBSession& db, Decoder& input,
                     Encoder& output) = 0;
};

class WriteAppBase : public AppBase {
 public:
  AppMode mode() const override { return AppMode::kWrite; }

  AppType type() const override { return AppType::kCppProcedure; }

  bool run(GraphDBSession& db, Decoder& input, Encoder& output) override {
    return this->Query(db, input, output);
  }

  virtual bool Query(GraphDBSession& db, Decoder& input, Encoder& output) = 0;
};

// for cypher procedure
template <typename... ARGS>
class CypherReadAppBase : public AppBase {
 public:
  AppType type() const override { return AppType::kCypherProcedure; }

  virtual results::CollectiveResults Query(const GraphDBSession& db,
                                           ARGS... args) = 0;

  bool Query(const GraphDBSession& db, Decoder& input,
             Encoder& output) override {
    auto sv = input.get_string();

    auto tuple = deserialize<ARGS...>(sv);
    if (!tuple) {
      return false;
    }

    // unpack tuple
    auto res = unpackedAndInvoke(tuple);
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

 private:
  GraphDBSession& graph_;
};

template <typename... ARGS>
class CypherWriteAppBase : public AppBase {
 public:
  AppType type() const override { return AppType::kCypherProcedure; }

  virtual bool Query(GraphDBSession& db, ARGS... args) = 0;

  bool Query(GraphDBSession& db, Decoder& input, Encoder& output) override {
    auto sv = input.get_string();

    auto tuple = deserialize<ARGS...>(sv);
    if (!tuple) {
      return false;
    }

    // unpack tuple
    unpackedAndInvoke(db, tuple);
    return true;
  }

  void unpackedAndInvoke(GraphDBSession& db, std::tuple<ARGS...>& tuple) {
    std::apply([this, &db](ARGS... args) { this->Query(db, args...); }, tuple);
  }
};
}  // namespace gs

#endif  // ENGINES_HQPS_DB_APP_INTERACTIVE_APP_BASE_H_