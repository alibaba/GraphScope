#pragma once

#include <stdarg.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <unistd.h>
#define gettid() syscall(SYS_gettid)

#define ODPS_STORAGE_API_LOG_DEBUG 0
#define ODPS_STORAGE_API_LOG_INFO 1
#define ODPS_STORAGE_API_LOG_ERROR 2

namespace apsara {
namespace odps {
namespace sdk {

class LogMessage {
 public:
  void Log(int level, const char* file, int line, const char* fmt, ...) {
    if (level < level_) {
      return;
    }
    struct tm tm_info;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    localtime_r(&tv.tv_sec, &tm_info);
    char time_str[30] = {'\0'};
    strftime(time_str, sizeof(time_str), "%Y:%m:%d %H:%M:%S", &tm_info);
    char buf[1024] = {0};
    snprintf(buf, sizeof(buf), "[%s.%03ld][%ld]%s:%d: %s", time_str,
             tv.tv_usec / 1000, (long) gettid(), file, line, fmt);
    va_list va;
    va_start(va, fmt);
    vfprintf(stdout, buf, va);
    va_end(va);
  }
  void SetLevel(uint32_t level) {
    if (level > ODPS_STORAGE_API_LOG_ERROR) {
      Log(ODPS_STORAGE_API_LOG_ERROR, __FILE__, __LINE__,
          "log level value is invalid: %d, reset to "
          "ODPS_STORAGE_API_LOG_ERROR\n",
          level);
      level_ = ODPS_STORAGE_API_LOG_ERROR;
      return;
    }
    level_ = level;
  }
  static LogMessage* GetInstance() {
    static LogMessage log_;
    return &log_;
  }

 private:
  LogMessage() {}
  int level_ = ODPS_STORAGE_API_LOG_ERROR;
};

}  // namespace sdk
}  // namespace odps
}  // namespace apsara

#define ODPS_LOG_INFO(fmt, ...)                                   \
  {                                                               \
    auto log_ = apsara::odps::sdk::LogMessage::GetInstance();     \
    log_->Log(ODPS_STORAGE_API_LOG_INFO, __FILE__, __LINE__, fmt, \
              ##__VA_ARGS__);                                     \
  }

#define ODPS_LOG_DEBUG(fmt, ...)                                   \
  {                                                                \
    auto log_ = apsara::odps::sdk::LogMessage::GetInstance();      \
    log_->Log(ODPS_STORAGE_API_LOG_DEBUG, __FILE__, __LINE__, fmt, \
              ##__VA_ARGS__);                                      \
  }

#define ODPS_LOG_ERROR(fmt, ...)                                   \
  {                                                                \
    auto log_ = apsara::odps::sdk::LogMessage::GetInstance();      \
    log_->Log(ODPS_STORAGE_API_LOG_ERROR, __FILE__, __LINE__, fmt, \
              ##__VA_ARGS__);                                      \
  }
