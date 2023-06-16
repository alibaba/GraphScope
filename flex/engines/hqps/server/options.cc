#include "flex/engines/hqps/server/options.h"

namespace snb::ic {

uint32_t shard_query_concurrency = 16;
uint32_t shard_update_concurrency = 4;
uint32_t shard_adhoc_concurrency = 4;

}  // namespace snb::ic
