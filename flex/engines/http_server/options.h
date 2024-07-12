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

#ifndef ENGINES_HTTP_SERVER_OPTIONS_H_
#define ENGINES_HTTP_SERVER_OPTIONS_H_

#include <cstdint>
#include <limits>

namespace server {

/// make update executors with higher priority.
const uint32_t interactive_admin_group_id = 1;
const uint32_t ic_query_group_id = 2;
const uint32_t ic_update_group_id = 3;
const uint32_t ic_adhoc_group_id = 4;
const uint32_t codegen_group_id = 5;
const uint32_t proc_query_group_id = 6;
const uint32_t proxy_group_id = 7;

const uint32_t max_group_id = std::numeric_limits<uint32_t>::max();
const uint32_t group_inc_step =
    5;  // should equal to number of non-admin groups.
// Each time we cancel a scope, we will increase the group id by this step.

extern uint32_t shard_query_concurrency;
extern uint32_t shard_update_concurrency;
extern uint32_t shard_adhoc_concurrency;
extern uint32_t shard_admin_graph_concurrency;
extern uint32_t shard_admin_node_concurrency;
extern uint32_t shard_admin_service_concurrency;
extern uint32_t shard_admin_job_concurrency;
extern uint32_t shard_admin_procedure_concurrency;
extern uint32_t shard_proxy_concurrency;

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_OPTIONS_H_
