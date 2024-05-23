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
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/proto_generated_gie/results.pb.h"
#include "flex/utils/property/types.h"
#include "nlohmann/json.hpp"
namespace gs {

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

// for cypher procedure
template <typename... ARGS>
class TypedInteractiveApp : public AppBase {
 public:
  TypedInteractiveApp(GraphDBSession& graph) : graph_(graph) {}

  std::string type() const override { return "cypher procedure"; }

  bool Query(Decoder& input, Encoder& output) override {
    //
    auto sv = input.get_string();
    char protocol = sv.back();
    size_t len = input.size();
    input.reset(input.data(), len - 1);

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

  results::CollectiveResults unpackedAndInvoke(std::tuple<ARGS...>& tuple) {
    return std::apply([this](ARGS... args) { return this->QueryImpl(args...); },
                      tuple);
  }

  virtual results::CollectiveResults QueryImpl(ARGS... args) = 0;

 private:
  GraphDBSession& graph_;
};

// for c++ procedure
class UnTypedInteractiveApp : public AppBase {
 public:
  UnTypedInteractiveApp(GraphDBSession& graph) : graph_(graph) {}

  std::string type() const override { return "c++ procedure"; }

  bool Query(Decoder& input, Encoder& output) override {
    return QueryImpl(input, output);
  }

  virtual bool QueryImpl(Decoder& decoder, Encoder& encoder) = 0;

 private:
  GraphDBSession& graph_;
};

}  // namespace gs

#endif  // ENGINES_HQPS_DB_APP_INTERACTIVE_APP_BASE_H_