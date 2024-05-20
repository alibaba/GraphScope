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

namespace gs {

template <typename... ARGS>
class InteractiveAppBase : public AppBase {
 public:
  InteractiveAppBase(GraphDBSession& graph);

  bool Query(Decoder& input, Encoder& output) override {
    //
    auto tuple = input.template Get<ARGS...>();
    if (!tuple) {
      return false;
    }
    // unpack tuple
    auto res = QueryImpl(tuple, output);
    // write output
    output.put_string_view(res.DebugString());
    return true;
  }

  results::Collection QueryImpl(std::tuple<ARGS...>& tuple) {
    return std::apply(
        [this, &output](ARGS... args) { return this->QueryImpl(args...); },
        tuple);
  }

  virtual results::Collection QueryImpl(ARGS... args) = 0;

 private:
  InteractiveAppBase& graph_;
};

}  // namespace gs

#endif  // ENGINES_HQPS_DB_APP_INTERACTIVE_APP_BASE_H_