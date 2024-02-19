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

#include "flex/engines/graph_db/database/compact_transaction.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/engines/graph_db/database/wal.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

CompactTransaction::CompactTransaction(MutablePropertyFragment& graph,
                                       WalWriter& logger, VersionManager& vm,
                                       timestamp_t timestamp)
    : graph_(graph), logger_(logger), vm_(vm), timestamp_(timestamp) {
  arc_.Resize(sizeof(WalHeader));
}

CompactTransaction::~CompactTransaction() { Abort(); }

timestamp_t CompactTransaction::timestamp() const { return timestamp_; }

void CompactTransaction::Commit() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
    header->length = 0;
    header->timestamp = timestamp_;
    header->type = 1;

    logger_.append(arc_.GetBuffer(), arc_.GetSize());
    arc_.Clear();

    LOG(INFO) << "before compact - " << timestamp_;
    graph_.Compact(timestamp_);
    LOG(INFO) << "after compact - " << timestamp_;

    vm_.release_update_timestamp(timestamp_);
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

void CompactTransaction::Abort() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    arc_.Clear();
    vm_.revert_update_timestamp(timestamp_);
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

}  // namespace gs