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

#ifndef ENGINES_BSP_APPS_H_
#define ENGINES_BSP_APPS_H_

#include "grape/analytical_apps/bfs/bfs.h"
#include "grape/analytical_apps/cdlp/cdlp.h"
#include "grape/analytical_apps/lcc/lcc.h"
#include "grape/analytical_apps/pagerank/pagerank.h"
#include "grape/analytical_apps/sssp/sssp.h"
#include "grape/analytical_apps/wcc/wcc.h"

namespace bsp {

template <typename FRAG_T>
using BFSApp = grape::BFS<FRAG_T>;

template <typename FRAG_T>
using SSSPApp = grape::SSSP<FRAG_T>;

template <typename FRAG_T>
using WCCApp = grape::WCC<FRAG_T>;

template <typename FRAG_T>
using PRApp = grape::PageRank<FRAG_T>;

template <typename FRAG_T>
using CDLPApp = grape::CDLP<FRAG_T>;

template <typename FRAG_T>
using LCCApp = grape::LCC<FRAG_T>;

}  // namespace bsp

#endif  // ENGINES_BSP_APPS_H_
