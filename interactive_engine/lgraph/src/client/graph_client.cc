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

#include <fstream>
#include "lgraph/client/graph_client.h"
#include "lgraph/common/check.h"

namespace LGRAPH_NAMESPACE {
namespace client {

Schema GraphClient::GetGraphSchema() {
  grpc::ClientContext ctx;
  GetSchemaRequest request;
  GetSchemaResponse response;
  grpc::Status s = client_stub_->getSchema(&ctx, request, &response);
  Check(s.ok(), "Get graph schema failed!");
  return Schema::FromProto(response.graphdef());
}

Schema GraphClient::LoadJsonSchema(const char *json_schema_file) {
  std::ifstream infile(json_schema_file);
  std::vector<char> buffer;
  infile.seekg(0, infile.end);
  long length = infile.tellg();
  Check(length > 0, "Loading empty json schema file!");
  buffer.resize(length);
  infile.seekg(0, infile.beg);
  infile.read(&buffer[0], length);
  infile.close();

  grpc::ClientContext ctx;
  LoadJsonSchemaRequest request;
  request.set_schemajson(std::string{&buffer[0], static_cast<size_t>(length)});
  LoadJsonSchemaResponse response;
  grpc::Status s = client_stub_->loadJsonSchema(&ctx, request, &response);
  Check(s.ok(), "Load graph schema from json file " + std::string{json_schema_file} + " failed!");
  return Schema::FromProto(response.graphdef());
}

LoggerInfo GraphClient::GetLoggerInfo() {
  grpc::ClientContext ctx;
  GetLoggerInfoRequest request;
  GetLoggerInfoResponse response;
  grpc::Status s = client_stub_->getLoggerInfo(&ctx, request, &response);
  Check(s.ok(), "Get logger info failed!");
  return LoggerInfo{response.loggerservers(), response.loggertopic(), response.loggerqueuecount()};
}

int32_t GraphClient::GetPartitionNum() {
  grpc::ClientContext ctx;
  GetPartitionNumRequest request;
  GetPartitionNumResponse response;
  grpc::Status s = client_stub_->getPartitionNum(&ctx, request, &response);
  Check(s.ok(), "Get partition number failed!");
  return response.partitionnum();
}

BackupId GraphClient::CreateNewBackup() {
  grpc::ClientContext ctx;
  CreateNewGraphBackupRequest request;
  CreateNewGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->createNewGraphBackup(&ctx, request, &response);
  Check(s.ok(), "Get logger info failed!");
  return response.backupid();
}

void GraphClient::DeleteBackup(BackupId backup_id) {
  grpc::ClientContext ctx;
  DeleteGraphBackupRequest request;
  request.set_backupid(backup_id);
  DeleteGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->deleteGraphBackup(&ctx, request, &response);
  Check(s.ok(), "Delete backup [" + std::to_string(backup_id) + "] failed!");
}

void GraphClient::PurgeOldBackups(int32_t keep_alive_num) {
  grpc::ClientContext ctx;
  PurgeOldGraphBackupsRequest request;
  request.set_keepalivenumber(keep_alive_num);
  PurgeOldGraphBackupsResponse response;
  grpc::Status s = client_backup_stub_->purgeOldGraphBackups(&ctx, request, &response);
  Check(s.ok(), "Purge old backups with keep_alive_num = " + std::to_string(keep_alive_num) + " failed!");
}

bool GraphClient::VerifyBackup(BackupId backup_id) {
  grpc::ClientContext ctx;
  VerifyGraphBackupRequest request;
  request.set_backupid(backup_id);
  VerifyGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->verifyGraphBackup(&ctx, request, &response);
  Check(s.ok(), "Verify backup [" + std::to_string(backup_id) + "] failed!");
  return response.isok();
}

std::vector<BackupInfo> GraphClient::GetBackupInfoList() {
  grpc::ClientContext ctx;
  GetGraphBackupInfoRequest request;
  GetGraphBackupInfoResponse response;
  grpc::Status s = client_backup_stub_->getGraphBackupInfo(&ctx, request, &response);
  Check(s.ok(), "Get backup info list failed!");
  std::vector<BackupInfo> backup_info_list;
  backup_info_list.reserve(response.backupinfolist_size());
  for (auto &backup_info_proto : response.backupinfolist()) {
    backup_info_list.emplace_back(std::move(BackupInfo::FromProto(backup_info_proto)));
  }
  return backup_info_list;
}

void GraphClient::RestoreFromBackup(BackupId backup_id, std::string meta_restore_path, std::string store_restore_path) {
  grpc::ClientContext ctx;
  RestoreFromGraphBackupRequest request;
  request.set_backupid(backup_id);
  request.set_meta_restore_path(meta_restore_path);
  request.set_store_restore_path(store_restore_path);
  RestoreFromGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->restoreFromGraphBackup(&ctx, request, &response);
  Check(s.ok(),
        "Restore from backup [" + std::to_string(backup_id) + "] at meta_path["
        + meta_restore_path + "] and store_path[" + store_restore_path + "] failed!");
}

}
}
