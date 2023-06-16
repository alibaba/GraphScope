#ifndef CORE_OPTIONS_H_
#define CORE_OPTIONS_H_

#include <cstdint>

namespace snb::ic {

/// make update executors with higher priority.
const uint32_t ic_query_group_id = 1;
const uint32_t ic_update_group_id = 2;
const uint32_t ic_adhoc_group_id = 3;

extern uint32_t shard_query_concurrency;
extern uint32_t shard_update_concurrency;
extern uint32_t shard_adhoc_concurrency;;

}  // namespace snb::ic

#endif  // CORE_OPTIONS_H_
