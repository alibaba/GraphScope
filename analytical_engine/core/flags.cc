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
#include <gflags/gflags.h>

#include "core/flags.h"

/* flags related to the job. */

DEFINE_string(host, "localhost", "the host to listen by gRPC server");
DEFINE_int32(port, 60001, "the port to listen by gRPC server");

// for vineyard
DEFINE_string(vineyard_socket, "", "Unix domain socket path for vineyardd");
DEFINE_string(vineyard_shared_mem, "2048000000",
              "Init size of vineyard shared memory");
DEFINE_string(etcd_endpoint, "http://127.0.0.1:2379",
              "Etcd endpoint that will be used to launch vineyardd");

DEFINE_string(dag_file, "", "Engine reads serialized dag proto from dag_file.");
