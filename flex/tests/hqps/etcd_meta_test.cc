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

#include "flex/storages/metadata/etcd_metadata_store.h"

#include <time.h>

#define CHECK_OK(res) CHECK(res.ok()) << res.status().error_message()

void test_meta_store(std::shared_ptr<gs::ETCDMetadataStore> store) {
  // 0. Remove all meta
  auto res0 = store->DeleteAllMeta("graph");
  CHECK_OK(res0);

  // 1. Create meta and get
  auto res1 = store->CreateMeta("graph", "graph_1");
  // check or print error message
  CHECK_OK(res1);
  auto res2 = store->CreateMeta("graph", "2", "graph_2");
  CHECK_OK(res2);
  auto res3 = store->GetMeta("graph", res1.value());
  CHECK_OK(res3);
  CHECK(res3.value() == "graph_1") << res3.value();
  auto res4 = store->GetMeta("graph", "2");
  CHECK_OK(res4);
  CHECK(res4.value() == "graph_2") << res4.value();

  // 2. Get all meta
  auto res5 = store->GetAllMeta("graph");
  CHECK_OK(res5);
  CHECK(res5.value().size() == 2) << res5.value().size();
  CHECK(res5.value()[0].second == "graph_1" &&
        res5.value()[1].second == "graph_2")
      << res5.value()[0].second << res5.value()[1].second;

  // 3. Update Meta
  auto res6 = store->UpdateMeta("graph", "1", "graph_1_updated");
  CHECK_OK(res6);
  auto res7 = store->GetMeta("graph", "1");
  CHECK_OK(res7);
  CHECK(res7.value() == "graph_1_updated");
  // Update with function
  auto res8 = store->UpdateMeta("graph", "2", [](const std::string& value) {
    return value + "_updated";
  });
  CHECK_OK(res8);
  auto res9 = store->GetMeta("graph", "2");
  CHECK_OK(res9);
  CHECK(res9.value() == "graph_2_updated");

  // 4. Delete Meta
  auto res10 = store->DeleteMeta("graph", "1");
  CHECK_OK(res10);
  auto res11 = store->GetMeta("graph", "1");
  CHECK(!res11.ok());

  // 5. Delete All Meta
  auto res12 = store->DeleteAllMeta("graph");
  CHECK_OK(res12);
  auto res13 = store->GetAllMeta("graph");
  CHECK_OK(res13);
  CHECK(res13.value().size() == 0) << res13.value().size();
  auto res14 = store->GetMeta("graph", "2");
  CHECK(!res14.ok());
}

int main(int argc, char** argv) {
  // Expect args etcd_meta_path.
  if (argc != 2) {
    LOG(ERROR) << "Usage: ./etcd_meta_test <etcd_meta_path>";
    return 1;
  }
  auto etcd_meta_path = std::string(argv[1]);

  // First delete all meta in etcd.
  auto base_url_and_root_uri = gs::extractBaseUrlAndMetaRootUri(etcd_meta_path);
  etcd::SyncClient client(base_url_and_root_uri.first);
  client.rmdir(base_url_and_root_uri.second, true);

  std::shared_ptr<gs::ETCDMetadataStore> store =
      std::make_shared<gs::ETCDMetadataStore>(etcd_meta_path);
  auto res = store->Open();
  if (!res.ok()) {
    LOG(ERROR) << "Failed to open etcd metadata store: "
               << res.status().error_message();
    return 1;
  }
  test_meta_store(store);

  LOG(INFO) << "Finish etcd_meta test.";
  return 0;
}
