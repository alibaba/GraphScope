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

#ifndef FLEX_UTILS_REMOTE_REMOTE_STORAGE_H_
#define FLEX_UTILS_REMOTE_REMOTE_STORAGE_H_

#include <string>
#include "flex/utils/result.h"

namespace gs {

class RemoteStorageWriter {
 public:
  virtual ~RemoteStorageWriter() = default;

  /**
   * Open a remote storage for writing.
   * @return Status
   */
  virtual Status Open() = 0;

  /**
   * Put a local file to the remote storage.
   * @param local_path The local file path.
   * @param remote_path The remote file path.
   * @param override If true, the file will be override if it exists.
   * @return Status
   * @note The local path could be a directory or a file.
   */
  virtual Status Put(const std::string& local_path,
                     const std::string& remote_path, bool override = false) = 0;

  /**
   * Delete a file from the remote storage.
   * @param remote_path The remote file path.
   * @return Status
   */
  virtual Status Delete(const std::string& remote_path) = 0;

  /**
   * Close the remote storage.
   */
  virtual Status Close() = 0;
};

class RemoteStorageReader {
 public:
  virtual ~RemoteStorageReader() = default;

  /**
   * Open a remote storage for reading.
   * @param uri The uri of the remote storage.
   * @return Status
   */
  virtual Status Open() = 0;

  /**
   * Get a file from the remote storage.
   * @param remote_path The remote file path.
   * @param local_path The local file path.
   * @return Status
   */
  virtual Status Get(const std::string& remote_path,
                     const std::string& local_path) = 0;

  /**
   * List all files in the remote storage.
   * @param remote_path The remote path.
   * @param files The files in the remote path.
   * @return Status
   */
  virtual Status List(const std::string& remote_path,
                      std::vector<std::string>& files) = 0;

  /**
   * Close the remote storage.
   */
  virtual Status Close() = 0;
};

}  // namespace gs

#endif  // FLEX_UTILS_REMOTE_REMOTE_STORAGE_H_