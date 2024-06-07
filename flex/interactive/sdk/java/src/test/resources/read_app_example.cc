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

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"
#include "flex/utils/app_utils.h"

namespace gs {
class ReadExample : public ReadAppBase {
 public:
  ReadExample() {}
  // Query function for query class
  bool Query(const gs::GraphDBSession& sess, Decoder& input,
             Encoder& output) override {
    // Expect a int input, return the same int
    int32_t input_int = input.get_int();
    output.put_int(input_int);
    return true;
  }
};
}  // namespace gs

extern "C" {
void* CreateApp(gs::GraphDBSession& db) {
  gs::ReadExample* app = new gs::ReadExample();
  return static_cast<void*>(app);
}

void DeleteApp(void* app) {
  gs::ReadExample* casted = static_cast<gs::ReadExample*>(app);
  delete casted;
}
}