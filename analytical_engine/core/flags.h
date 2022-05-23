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

#ifndef ANALYTICAL_ENGINE_CORE_FLAGS_H_
#define ANALYTICAL_ENGINE_CORE_FLAGS_H_

#include <gflags/gflags_declare.h>

DECLARE_string(host);
DECLARE_int32(port);

DECLARE_string(dag_file);

// vineyard
DECLARE_string(vineyard_socket);
DECLARE_string(vineyard_shared_mem);
DECLARE_string(etcd_endpoint);

#endif  // ANALYTICAL_ENGINE_CORE_FLAGS_H_
