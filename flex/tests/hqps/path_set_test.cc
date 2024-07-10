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
#include <climits>
#include <vector>
#include "flex/engines/hqps_db/core/context.h"
#include "flex/engines/hqps_db/core/params.h"
#include "flex/engines/hqps_db/structures/multi_vertex_set/row_vertex_set.h"
#include "flex/storages/rt_mutable_graph/types.h"

#include "flex/engines/hqps_db/structures/path.h"

#include "grape/types.h"

#include "glog/logging.h"

namespace gs {

void test_path_set() {
  std::vector<std::vector<vid_t>> vids;
  std::vector<std::vector<offset_t>> offsets;
  std::vector<label_t> label_ids;

  vids.emplace_back(std::vector<vid_t>{1, 2});
  offsets.emplace_back(std::vector<offset_t>{0, 1, 2});

  vids.emplace_back(std::vector<vid_t>{3, 4, 5, 6});
  offsets.emplace_back(std::vector<offset_t>{0, 2, 4});

  vids.emplace_back(std::vector<vid_t>{7, 8, 9, 10});
  offsets.emplace_back(std::vector<offset_t>{0, 1, 1, 3, 4});

  label_ids = std::vector<label_t>{1, 1, 1};
  auto compressed_set = CompressedPathSet<vid_t, label_t>(
      std::move(vids), std::move(offsets), std::move(label_ids), 1);

  std::vector<Path<vid_t, label_t>> paths;
  paths.emplace_back(
      Path<vid_t, label_t>(std::vector<vid_t>{1}, std::vector<label_t>{1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{1, 3},
                                          std::vector<label_t>{1, 1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{1, 4},
                                          std::vector<label_t>{1, 1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{1, 3, 7},
                                          std::vector<label_t>{1, 1, 1}));
  paths.emplace_back(
      Path<vid_t, label_t>(std::vector<vid_t>{2}, std::vector<label_t>{1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{2, 5},
                                          std::vector<label_t>{1, 1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{2, 6},
                                          std::vector<label_t>{1, 1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{2, 5, 8},
                                          std::vector<label_t>{1, 1, 1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{2, 5, 9},
                                          std::vector<label_t>{1, 1, 1}));
  paths.emplace_back(Path<vid_t, label_t>(std::vector<vid_t>{2, 6, 10},
                                          std::vector<label_t>{1, 1, 1}));

  CHECK(compressed_set.Size() == paths.size())
      << "Size not equal." << compressed_set.Size() << ", " << paths.size();
  size_t cnt = 0;
  for (auto iter : compressed_set) {
    auto path = iter.GetElement();
    CHECK(path == paths[cnt]) << "Path not equal." << path.to_string() << ", "
                              << paths[cnt].to_string();
    VLOG(10) << "got path:" << gs::to_string(path);
    cnt += 1;
  }
  LOG(INFO) << "Finish path set test.";
}
}  // namespace gs

int main() { gs::test_path_set(); }
