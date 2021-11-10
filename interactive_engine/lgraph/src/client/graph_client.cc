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

#include "client/graph_client.h"
#include "common/check.h"

namespace LGRAPH_NAMESPACE {
namespace client {

LoggerInfo GraphClient::GetLoggerInfo() {
  GetLoggerInfoRequest request;
  GetLoggerInfoResponse response;
  grpc::Status s = client_stub_->getLoggerInfo(&ctx_, request, &response);
  Check(s.ok(), "Get logger info failed!");
  return LoggerInfo{response.loggerservers(), response.loggertopic(), response.loggerqueuecount()};
}

BackupId GraphClient::CreateNewBackup() {
  CreateNewGraphBackupRequest request;
  CreateNewGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->createNewGraphBackup(&ctx_, request, &response);
  Check(s.ok(), "Get logger info failed!");
  return response.backupid();
}

void GraphClient::DeleteBackup(BackupId backup_id) {
  DeleteGraphBackupRequest request;
  request.set_backupid(backup_id);
  DeleteGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->deleteGraphBackup(&ctx_, request, &response);
  Check(s.ok(), "Delete backup [" + std::to_string(backup_id) + "] failed!");
}

void GraphClient::PurgeOldBackups(int32_t keep_alive_num) {
  PurgeOldGraphBackupsRequest request;
  request.set_keepalivenumber(keep_alive_num);
  PurgeOldGraphBackupsResponse response;
  grpc::Status s = client_backup_stub_->purgeOldGraphBackups(&ctx_, request, &response);
  Check(s.ok(), "Purge old backups with keep_alive_num = " + std::to_string(keep_alive_num) + " failed!");
}

bool GraphClient::VerifyBackup(BackupId backup_id) {
  VerifyGraphBackupRequest request;
  request.set_backupid(backup_id);
  VerifyGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->verifyGraphBackup(&ctx_, request, &response);
  Check(s.ok(), "Verify backup [" + std::to_string(backup_id) + "] failed!");
  return response.isok();
}

std::vector<BackupInfo> GraphClient::GetBackupInfoList() {
  GetGraphBackupInfoRequest request;
  GetGraphBackupInfoResponse response;
  grpc::Status s = client_backup_stub_->getGraphBackupInfo(&ctx_, request, &response);
  Check(s.ok(), "Get backup info list failed!");
  std::vector<BackupInfo> backup_info_list;
  backup_info_list.reserve(response.backupinfolist_size());
  for (auto &backup_info_proto : response.backupinfolist()) {
    backup_info_list.emplace_back(std::move(BackupInfo::FromProto(backup_info_proto)));
  }
  return backup_info_list;
}

void GraphClient::RestoreFromBackup(BackupId backup_id, std::string meta_restore_path, std::string store_restore_path) {
  RestoreFromGraphBackupRequest request;
  request.set_backupid(backup_id);
  request.set_meta_restore_path(meta_restore_path);
  request.set_store_restore_path(store_restore_path);
  RestoreFromGraphBackupResponse response;
  grpc::Status s = client_backup_stub_->restoreFromGraphBackup(&ctx_, request, &response);
  Check(s.ok(),
        "Restore from backup [" + std::to_string(backup_id) + "] at meta_path["
        + meta_restore_path + "] and store_path[" + store_restore_path + "] failed!");
}

}
}
