#ifndef CORE_EXECUTOR_GROUP_ACTG_H_
#define CORE_EXECUTOR_GROUP_ACTG_H_

#include <hiactor/core/actor-template.hh>

namespace snb::ic {

class ANNOTATION(actor:group) executor_group : public hiactor::schedulable_actor_group {
public:
  executor_group(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
    : hiactor::schedulable_actor_group(exec_ctx, addr) {}

  bool compare(const actor_base* a, const actor_base* b) const override {
    /// Larger actor id will have higher priority
    return a->actor_id() < b->actor_id();
  }
};

}  // namespace snb::ic

#endif  // CORE_EXECUTOR_GROUP_ACTG_H_
