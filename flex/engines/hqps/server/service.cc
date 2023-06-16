#include "flex/engines/hqps/server/service.h"
#include "flex/engines/hqps/server/options.h"
namespace snb::ic {

void service::init(uint32_t num_shards, uint16_t http_port, bool dpdk_mode) {
  actor_sys_ = std::make_unique<actor_system>(num_shards, dpdk_mode);
  http_hdl_ = std::make_unique<http_handler>(http_port);
}

service::~service() {
  if (actor_sys_) {
    actor_sys_->terminate();
  }
}

void service::run_and_wait_for_exit() {
  if (!actor_sys_ || !http_hdl_) {
    std::cerr << "Service has not been inited!" << std::endl;
    return;
  }
  actor_sys_->launch();
  http_hdl_->start();
  running_.store(true);
  while (running_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  http_hdl_->stop();
  actor_sys_->terminate();
}

void service::set_exit_state() {
  running_.store(false);
}

}  // namespace snb::ic
