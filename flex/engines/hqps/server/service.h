#ifndef CORE_SERVICE_H_
#define CORE_SERVICE_H_

#include <string>

#include "flex/engines/hqps/server/actor_system.h"
#include "flex/engines/hqps/server/http_handler.h"

#include "flex/storages/mutable_csr/grape_graph_interface.h"

namespace snb::ic {

class service {
public:
  static service& get() {
    static service instance;
    return instance;
  }
  ~service();

  // the store procedure contains <query_id, query_name, store_path>
  void init(uint32_t num_shards, uint16_t http_port, bool dpdk_mode);

  void run_and_wait_for_exit();
  void set_exit_state();

private:
  service() = default;

private:
  std::unique_ptr<actor_system> actor_sys_;
  std::unique_ptr<http_handler> http_hdl_;
  std::atomic<bool> running_{false};
};

}  // namespace snb::ic

#endif  // CORE_SERVICE_H_
