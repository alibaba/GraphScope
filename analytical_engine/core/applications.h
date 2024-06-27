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

#include <cstdio>
#include <fstream>
#include <string>

#include "common/util/uuid.h"
#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"

#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"

namespace gs {

template <typename FRAG_T>
std::shared_ptr<FRAG_T> LoadSimpleGraph(const std::string& efile,
                                        const std::string& vfile,
                                        const grape::CommSpec& comm_spec);

template <typename OID_T = int64_t, typename VID_T = uint64_t>
vineyard::ObjectID LoadPropertyGraph(const grape::CommSpec& comm_spec,
                                     vineyard::Client& client,
                                     const std::vector<std::string>& efiles,
                                     const std::vector<std::string>& vfiles,
                                     int directed);

template <typename FRAG_T, typename PROJECT_FRAG_T>
std::shared_ptr<PROJECT_FRAG_T> ProjectGraph(std::shared_ptr<FRAG_T> fragment);

template <typename FRAG_T, typename APP_T, typename... Args>
void DoQuery(const grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
             const std::string& out_prefix, Args... args);

template <typename FRAG_T>
void RunProjectedApp(std::shared_ptr<FRAG_T> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix, const std::string& name);

template <typename FRAG_T>
void RunPropertyApp(std::shared_ptr<FRAG_T> fragment,
                    const grape::CommSpec& comm_spec,
                    const std::string& out_prefix, const std::string& name);
std::vector<int> prepareSamplingPathPattern(const std::string& path_pattern);

void RunApp();
}  // namespace gs