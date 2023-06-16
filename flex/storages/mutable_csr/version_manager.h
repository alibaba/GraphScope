#ifndef GRAPHSCOPE_DATABASE_VERSION_MANAGER_H_
#define GRAPHSCOPE_DATABASE_VERSION_MANAGER_H_

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <bitset>
#include <thread>

#include "glog/logging.h"
#include "grape/grape.h"
#include "flex/engines/hqps/engine/utils/bitset.h"

namespace gs {

class VersionManager {
 public:
  VersionManager();
  ~VersionManager();

  void init_ts(uint32_t ts);

  void set_wait_visable(bool value);

  uint32_t acquire_read_timestamp();

  void release_read_timestamp();

  uint32_t acquire_insert_timestamp();
  void release_insert_timestamp(uint32_t ts);

  uint32_t acquire_update_timestamp();
  void release_update_timestamp(uint32_t ts);

  void update_read_version();

 private:
  std::atomic<uint32_t> write_ts_{1};
  std::atomic<uint32_t> read_ts_{0};
  uint32_t cursor_{1};

  std::atomic<int> pending_reqs_{0};

  Bitset buf_;

  std::thread update_read_thread_;
  std::atomic<bool> running_{true};

  bool wait_visable_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_VERSION_MANAGER_H_
