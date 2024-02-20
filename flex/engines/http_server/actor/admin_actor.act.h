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

#ifndef ENGINES_HTTP_SERVER_ACTOR_ADMIN_ACT_H_
#define ENGINES_HTTP_SERVER_ACTOR_ADMIN_ACT_H_

#include "flex/engines/http_server/types.h"
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include <mutex>

#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

namespace server {

class ANNOTATION(actor:impl) admin_actor : public hiactor::actor {
 public:
  admin_actor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr);
  ~admin_actor() override;

  seastar::future<query_result> ANNOTATION(actor:method) run_create_graph(query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) run_get_graph_schema(query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) run_list_graphs(query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) run_delete_graph(query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) run_graph_loading(graph_management_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) start_service(query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) service_status(query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) get_procedure_by_procedure_name(procedure_query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) get_procedures_by_graph_name(query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) create_procedure(create_procedure_query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) delete_procedure(procedure_query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) update_procedure(update_procedure_query_param&& param);

  seastar::future<query_result> ANNOTATION(actor:method) node_status(query_param&& param);

  // DECLARE_RUN_QUERIES;
  /// Declare `do_work` func here, no need to implement.
  ACTOR_DO_WORK()

 private:
  std::mutex mtx_;
};

}  // namespace server

#endif  // ENGINES_HTTP_SERVER_ACTOR_ADMIN_ACT_H_
