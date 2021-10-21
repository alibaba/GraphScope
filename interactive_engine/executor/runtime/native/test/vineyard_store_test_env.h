/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef VINEYARD_STORE_VINEYARD_STORE_TEST_ENV_H
#define VINEYARD_STORE_VINEYARD_STORE_TEST_ENV_H

#include <string>

#include <boost/process.hpp>
#include <boost/filesystem.hpp>

#include <gtest/gtest.h>

namespace vineyard_store_test {

class VineyardStoreTestEnv : public testing::Environment {
 public:
  ~VineyardStoreTestEnv() override = default;

  void SetUp() override;

  void TearDown() override;

 private:
  void StartVineyardd();

 private:
  boost::process::child etcd_proc_;
  boost::process::child vineyardd_proc_;
  boost::process::group vineyardd_group_;
  boost::filesystem::path test_root_dir_;
};


}

#endif //VINEYARD_STORE_VINEYARD_STORE_TEST_ENV_H
