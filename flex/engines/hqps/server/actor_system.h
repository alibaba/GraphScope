#ifndef CORE_ACTOR_SYSTEM_H_
#define CORE_ACTOR_SYSTEM_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <semaphore.h>
#include <thread>

namespace snb::ic {

class actor_system {
public:
  actor_system(uint32_t num_shards, bool enable_dpdk) : num_shards_(num_shards), enable_dpdk_(enable_dpdk) {}
  ~actor_system();

  void launch();
  void terminate();

private:
  void launch_worker();

private:
  const uint32_t num_shards_;
  const bool enable_dpdk_;
  std::unique_ptr<std::thread> main_thread_;
  std::atomic<bool> running_{false};
  sem_t ready_;
};

}  // namespace snb::ic

#endif  // CORE_ACTOR_SYSTEM_H_