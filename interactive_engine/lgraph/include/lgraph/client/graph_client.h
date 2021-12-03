/**
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <grpc++/grpc++.h>
#include "lgraph/common/backup_info.h"
#include "lgraph/common/logger_info.h"
#include "lgraph/proto/client.grpc.pb.h"
#include "lgraph/proto/client_backup_service.grpc.pb.h"

namespace LGRAPH_NAMESPACE {
namespace client {

class GraphClient {
public:
  explicit GraphClient(const std::string &target)
      : GraphClient(grpc::CreateChannel(target, grpc::InsecureChannelCredentials())) {}
  explicit GraphClient(const std::shared_ptr<grpc::Channel> &channel)
      : client_stub_(Client::NewStub(channel)), client_backup_stub_(ClientBackup::NewStub(channel)) {}
  ~GraphClient() = default;

  Schema GetGraphSchema();
  Schema LoadJsonSchema(const char *json_schema_file);
  LoggerInfo GetLoggerInfo();
  int32_t GetPartitionNum();
  BackupId CreateNewBackup();
  void DeleteBackup(BackupId backup_id);
  void PurgeOldBackups(int32_t keep_alive_num);
  bool VerifyBackup(BackupId backup_id);
  std::vector<BackupInfo> GetBackupInfoList();
  void RestoreFromBackup(BackupId backup_id, std::string meta_restore_path, std::string store_restore_path);

private:
  std::unique_ptr<Client::Stub> client_stub_;
  std::unique_ptr<ClientBackup::Stub> client_backup_stub_;
};

}
}
