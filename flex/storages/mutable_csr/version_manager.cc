#include "flex/storages/mutable_csr/version_manager.h"

#define likely(x) __builtin_expect(!!(x), 1)

namespace gs {

constexpr static uint32_t ring_buf_size = 1024 * 1024;
constexpr static uint32_t ring_index_mask = ring_buf_size - 1;

VersionManager::VersionManager() : wait_visable_(false) {
  buf_.init(ring_buf_size);
  update_read_thread_ = std::thread([this]() {
    while (running_.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
      update_read_version();
    }
  });
}
VersionManager::~VersionManager() {
  if (!wait_visable_) {
    running_.store(false);
    update_read_thread_.join();
  }
}

void VersionManager::init_ts(uint32_t ts) {
  write_ts_.store(ts + 1);
  read_ts_.store(ts);

  cursor_ = (ts + 1) & ring_index_mask;
}

void VersionManager::set_wait_visable(bool value) {
  if (value == wait_visable_) {
    return;
  }
  wait_visable_ = value;
  if (wait_visable_) {
    running_.store(false);
    update_read_thread_.join();
  }
}

uint32_t VersionManager::acquire_read_timestamp() {
  auto pr = pending_reqs_.fetch_add(1);
  if (likely(pr >= 0)) {
    return read_ts_.load();
  } else {
    while (true) {
      while (pending_reqs_.load() < 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      pr = pending_reqs_.fetch_add(1);
      if (pr >= 0) {
        return read_ts_.load();
      }
    }
  }
}

void VersionManager::release_read_timestamp() { pending_reqs_.fetch_sub(1); }

uint32_t VersionManager::acquire_insert_timestamp() {
  auto pr = pending_reqs_.fetch_add(1);
  if (likely(pr >= 0)) {
    return write_ts_.fetch_add(1);
  } else {
    while (true) {
      while (pending_reqs_.load() < 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      pr = pending_reqs_.fetch_add(1);
      if (pr >= 0) {
        return write_ts_.fetch_add(1);
      }
    }
  }
}
void VersionManager::release_insert_timestamp(uint32_t ts) {
  buf_.set_bit(ts & ring_index_mask);
  if (wait_visable_) {
    update_read_version();
  }
  pending_reqs_.fetch_sub(1);
}

uint32_t VersionManager::acquire_update_timestamp() {
  int expected = 0;
  while (!pending_reqs_.compare_exchange_strong(
      expected, std::numeric_limits<int>::min())) {
    expected = 0;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
  return write_ts_.fetch_add(1);
}
void VersionManager::release_update_timestamp(uint32_t ts) {
  buf_.set_bit(ts & ring_index_mask);
  pending_reqs_.store(0);
}

void VersionManager::update_read_version() {
  uint32_t ts = read_ts_.load();
  uint32_t old_ts = ts;
  while (buf_.get_bit(cursor_)) {
    buf_.reset_bit(cursor_);
    cursor_ = (cursor_ + 1) & ring_index_mask;
    ++ts;
  }
  if (old_ts != ts) {
    CHECK_LT(ts - old_ts, ring_buf_size);
    read_ts_.store(ts);
  }
}

}  // namespace gs

#undef likely
