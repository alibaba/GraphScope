/** Copyright 2021 Alibaba Group Holding Limited. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <hiactor/core/actor-app.hh>
#include <seastar/core/alien.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sleep.hh>

using namespace std::chrono_literals;

seastar::future<> simulate() {
  // create a default actor group with id "1" to manage food actors.
  return seastar::parallel_for_each(boost::irange<int>(0u, 6u), [](int i) {})
      .then([]() {
        // display info
        fmt::print("All food items are ready.\n");
        return seastar::sleep(1s);
      });
}

void blockSignal(int sig) {
  sigset_t set;
  // 初始化信号集
  sigemptyset(&set);
  // 将指定信号添加到信号集中
  sigaddset(&set, sig);
  // 将信号集中的信号添加到当前线程的信号掩码中，即阻塞这些信号
  if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
    perror("pthread_sigmask");
  }
}

void launch(std::function<void()> on_exit, int ac, char** av) {
  seastar::app_template::config conf;
  conf.auto_handle_sigint_sigterm = false;
  hiactor::actor_app app{std::move(conf)};
  app.run(
      ac, av,
      [on_exit = on_exit] {  // Explicitly capture the 'running' variable
        return simulate().then(
            [on_exit = on_exit] {  // Explicitly capture the 'running' variable
              seastar::engine().handle_signal(SIGINT, [on_exit = on_exit] {
                std::cerr << "sigint" << std::endl;
                on_exit();
              });
              fmt::print("Exit actor system.\n");
              // kill(getpid(), SIGINT);
            });
      });
}

int main(int ac, char** av) {
  // launch(ac, av);
  blockSignal(SIGINT);
  volatile bool running{true};
  std::unique_ptr<std::thread> main_thread_ = std::make_unique<std::thread>(
      launch, [&running]() { running = false; }, ac, av);
  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  seastar::alien::run_on(*seastar::alien::internal::default_instance, 0,
                         [] { hiactor::actor_engine().exit(); });
  main_thread_->join();
  return 0;
}
