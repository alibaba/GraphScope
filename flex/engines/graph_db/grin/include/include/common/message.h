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

/**
 @file message.h
 @brief Define storage feature protobuf message
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_COMMON_MESSAGE_H_
#define GRIN_INCLUDE_COMMON_MESSAGE_H_

/**
 * @brief Get the static feature prototype message of the storage.
 * This proto describes the features of the storage, such as whether
 * it supports property graph or partitioned graph.
 * @return The serialized proto message.
*/
const char* grin_get_static_storage_feature_msg();

#endif // GRIN_INCLUDE_PROTO_MESSAGE_H_

#ifdef __cplusplus
}
#endif