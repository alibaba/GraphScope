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

#include "gtest/gtest.h"

#include "vineyard_store_test_env.h"

using vineyard_store_test::VineyardStoreTestEnv;

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  VineyardStoreTestEnv* test_env = new VineyardStoreTestEnv();
  // google test takes ownership of test_env and release it
  ::testing::AddGlobalTestEnvironment(test_env);
  return RUN_ALL_TESTS();
}
