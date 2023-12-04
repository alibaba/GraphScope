#pragma once

#include <time.h>

#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <vector>

#include "common/log.h"

#define DURATION(start, end) \
  ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec))
#define DURATION_SEC(start, end) (end.tv_sec - start.tv_sec)

namespace apsara {
namespace odps {
namespace sdk {
namespace storage_api {
struct HaloRing {
  volatile size_t start;  // consumer update
  volatile size_t end;    // producer update
  void consume_all(std::function<void(const char* data, size_t size)> func) {
    size_t size = end - start;
    func(buf_ + (start % size_), size);
    end = start;
  }
  char* buf_;
  size_t size_;
};

// from httplib.h
class ThreadPool {
 public:
  explicit ThreadPool(size_t n) : shutdown_(false) {
    while (n) {
      auto t = std::thread(worker(*this));
      t.detach();
      threads_.emplace_back(std::move(t));
      // threads_.emplace_back(worker(*this));
      n--;
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ~ThreadPool() = default;

  void enqueue(std::function<void()> fn) {
    std::unique_lock<std::mutex> lock(mutex_);
    jobs_.push_back(std::move(fn));
    cond_.notify_one();
  }

  void shutdown() {
    // Stop all worker threads...
    {
      std::unique_lock<std::mutex> lock(mutex_);
      shutdown_ = true;
    }

    cond_.notify_all();

    // Join...
    for (auto& t : threads_) {
      t.join();
    }
  }

 private:
  struct worker {
    explicit worker(ThreadPool& pool) : pool_(pool) {}

    void operator()() {
      for (;;) {
        std::function<void()> fn;
        {
          std::unique_lock<std::mutex> lock(pool_.mutex_);

          pool_.cond_.wait(
              lock, [&] { return !pool_.jobs_.empty() || pool_.shutdown_; });

          if (pool_.shutdown_ && pool_.jobs_.empty()) {
            break;
          }

          fn = pool_.jobs_.front();
          pool_.jobs_.pop_front();
        }

        assert(true == static_cast<bool>(fn));
        fn();
      }
    }

    ThreadPool& pool_;
  };
  friend struct worker;

  std::vector<std::thread> threads_;
  std::list<std::function<void()>> jobs_;

  bool shutdown_;

  std::condition_variable cond_;
  std::mutex mutex_;
};

template <typename T>
class BlockingQueue {
 public:
  BlockingQueue(size_t limit = 5) : limit_(limit) {}
  bool Put(T t) {
    std::unique_lock<std::mutex> lock(mutex_);
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &start);
    full_cond_.wait(lock, [&]() {
      clock_gettime(CLOCK_MONOTONIC_COARSE, &now);
      if (acc_ >= limit_ && DURATION(start, now) > 10000000) {}
      return acc_ < limit_ || shutdown_;
    });
    if (shutdown_) {
      return false;
    }
    acc_ += 1;
    queue_.push_back(std::move(t));
    empty_cond_.notify_one();
    return true;
  }
  bool Get(T& t) {
    std::unique_lock<std::mutex> lock(mutex_);

    empty_cond_.wait(lock, [&]() { return !queue_.empty() || shutdown_; });

    if (shutdown_ && queue_.empty()) {
      if (acc_ != 0) {
        // HaloLog("!!!!!!!!!!!!!!!!!\n");
        throw "";
      }
      return false;
    }
    // HaloLog("queue is shutdown, but not empty\n");
    t = std::move(queue_.front());
    queue_.pop_front();
    acc_ -= 1;
    full_cond_.notify_one();
    return true;
  }

  void ShutDown() {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      shutdown_ = true;
    }
    empty_cond_.notify_all();
    full_cond_.notify_all();
  }

 private:
  std::list<T> queue_;

  bool shutdown_ = false;

  std::condition_variable empty_cond_;
  std::condition_variable full_cond_;
  std::mutex mutex_;
  size_t limit_ = 5;
  size_t acc_ = 0;
};

extern apsara::odps::sdk::storage_api::ThreadPool g_thread_pool;
}  // namespace storage_api
}  // namespace sdk
}  // namespace odps
}  // namespace apsara
