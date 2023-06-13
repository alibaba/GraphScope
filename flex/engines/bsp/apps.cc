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

#include "flex/engines/bsp/apps.h"
#include "flex/storages/immutable_graph/immutable_graph.h"

using WeightedGraph = immutable_graph::ImmutableGraph<
    int64_t, uint32_t, grape::EmptyType, double, grape::LoadStrategy::kOnlyOut,
    grape::GlobalVertexMap<int64_t, uint32_t,
                           grape::SegmentedPartitioner<int64_t>>>;
using NonWeightedGraph = immutable_graph::ImmutableGraph<
    int64_t, uint32_t, grape::EmptyType, grape::EmptyType,
    grape::LoadStrategy::kOnlyOut,
    grape::GlobalVertexMap<int64_t, uint32_t,
                           grape::SegmentedPartitioner<int64_t>>>;

template class grape::BFS<NonWeightedGraph>;
template class grape::SSSP<WeightedGraph>;
template class grape::WCC<NonWeightedGraph>;
template class grape::PageRank<NonWeightedGraph>;
template class grape::CDLP<NonWeightedGraph>;
