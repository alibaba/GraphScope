/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPHSCOPE_GRAPH_INDEXERS_H_
#define GRAPHSCOPE_GRAPH_INDEXERS_H_

#ifndef USE_PTHASH
#include "flex/utils/id_indexer.h"
#else
#include "flex/utils/pt_indexer.h"
#endif

namespace gs {

#ifndef USE_PTHASH
using IndexerType = LFIndexer<vid_t>;

template <typename KEY_T>
using IndexerBuilderType = IdIndexer<KEY_T, vid_t>;
#else
using IndexerType = PTIndexer<vid_t>;

template <typename KEY_T>
using IndexerBuilderType = PTIndexerBuilder<KEY_T, vid_t>;
#endif

}  // namespace gs

#endif  // GRAPHSCOPE_GRAPH_INDEXERS_H_
