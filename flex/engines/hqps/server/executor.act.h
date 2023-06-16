#ifndef CORE_EXECUTOR_ACT_H_
#define CORE_EXECUTOR_ACT_H_

#include "flex/engines/hqps/server/types.h"
#include <hiactor/core/actor-template.hh>
#include <hiactor/util/data_type.hh>

namespace snb::ic {

class ANNOTATION(actor:impl) executor : public hiactor::actor {
public:
  executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr);
  ~executor() override;
  
  seastar::future<query_result> ANNOTATION(actor:method) run_query(query_param&& param);

  // run adhoc query
  seastar::future<query_result> ANNOTATION(actor:method) run_adhoc_query(query_param&& param);

  //DECLARE_RUN_QUERYS;
  /// Declare `do_work` func here, no need to implement.
  ACTOR_DO_WORK()

private:
  int32_t your_private_members_ = 0;
};

}  // namespace snb::ic

#endif  // CORE_EXECUTOR_ACT_H_
