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

#ifndef ENGINES_HTTP_SERVER_ACTOR_EXECUTOR_ACT_H_
#define ENGINES_HTTP_SERVER_ACTOR_EXECUTOR_ACT_H_

#include "flex/engines/http_server/types.h"

#include "flex/storages/metadata/graph_meta_store.h"

#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

namespace server {

class ANNOTATION(actor:impl) executor : public hiactor::actor {
 public:
  executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr);
  ~executor() override;

  seastar::future<query_result> ANNOTATION(actor:method) run_graph_db_query(query_param&& param);
  
  seastar::future<admin_query_result> ANNOTATION(actor:method) create_vertex(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) create_edge(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) delete_vertex(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) delete_edge(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) update_vertex(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) update_edge(graph_management_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) get_vertex(graph_management_query_param&& param);

  seastar::future<admin_query_result> ANNOTATION(actor:method) get_edge(graph_management_query_param&& param);


  // DECLARE_RUN_QUERIES;
  /// Declare `do_work` func here, no need to implement.
  ACTOR_DO_WORK()

 private:
  int32_t your_private_members_ = 0;
  std::shared_ptr<gs::IGraphMetaStore> metadata_store_;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_ACTOR_EXECUTOR_ACT_H_
