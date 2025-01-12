/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef RUNTIME_ADHOC_RUNTIME_H_
#define RUNTIME_ADHOC_RUNTIME_H_

#include "flex/engines/graph_db/runtime/common/graph_interface.h"

namespace gs {

namespace runtime {

class TimerUnit {
 public:
  TimerUnit() = default;
  ~TimerUnit() = default;

  void start() {
#ifdef RT_PROFILE
    start_ = -grape::GetCurrentTime();
#endif
  }

  double elapsed() const {
#ifdef RT_PROFILE
    return start_ + grape::GetCurrentTime();
#else
    return 0;
#endif
  }

 private:
#ifdef RT_PROFILE
  double start_;
#endif
};

class OprTimer {
 public:
  OprTimer() = default;
  ~OprTimer() = default;

  void add_total(double time) {
#ifdef RT_PROFILE
    total_time_ += time;
#endif
  }

  void record_opr(const std::string& opr, double time) {
#ifdef RT_PROFILE
    opr_timers_[opr] += time;
#endif
  }

  void record_routine(const std::string& routine, double time) {
#ifdef RT_PROFILE
    routine_timers_[routine] += time;
#endif
  }

  void add_total(const TimerUnit& tu) {
#ifdef RT_PROFILE
    total_time_ += tu.elapsed();
#endif
  }

  void record_opr(const std::string& opr, const TimerUnit& tu) {
#ifdef RT_PROFILE
    opr_timers_[opr] += tu.elapsed();
#endif
  }

  void record_routine(const std::string& routine, const TimerUnit& tu) {
#ifdef RT_PROFILE
    routine_timers_[routine] += tu.elapsed();
#endif
  }

  void output(const std::string& path) const;

  void clear();

  OprTimer& operator+=(const OprTimer& other);

 private:
#ifdef RT_PROFILE
  std::map<std::string, double> opr_timers_;
  std::map<std::string, double> routine_timers_;
  double total_time_ = 0;
#endif
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_ADHOC_RUNTIME_H_