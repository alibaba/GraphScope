/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
#include <cstdlib>
#include <cstdio>
#include <string>

#include "graph_builder_ffi.h"
#include "htap_types.h"

static Schema MakeSchema() {
  auto schema = create_schema_builder();
  {
    auto v = build_vertex_type(schema, 1, "person");
    build_vertex_property(v, 1, "id", PropertyType::LONG);
    build_vertex_property(v, 2, "name", PropertyType::STRING);
    build_vertex_property(v, 3, "edge", PropertyType::INT);
    const char* ss[] = {"id"};
    build_vertex_primary_keys(v, 1, ss);
    finish_build_vertex(v);
  }
  {
    auto v = build_vertex_type(schema, 2, "software");
    build_vertex_property(v, 1, "id", PropertyType::LONG);
    build_vertex_property(v, 2, "name", PropertyType::STRING);
    build_vertex_property(v, 4, "lang", PropertyType::STRING);
    build_vertex_property(v, 5, "temp", PropertyType::STRING);
    const char* ss[] = {"id"};
    build_vertex_primary_keys(v, 1, ss);
    finish_build_vertex(v);
  }
  {
    auto e = build_edge_type(schema, 7, "knows");
    build_edge_property(e, 6, "weight", PropertyType::DOUBLE);
    build_edge_relation(e, "person", "person");
    finish_build_edge(e);
  }
  {
    auto e = build_edge_type(schema, 8, "created");
    build_edge_property(e, 6, "weight", PropertyType::DOUBLE);
    build_edge_relation(e, "person", "software");
    build_edge_relation(e, "software", "person");
    finish_build_edge(e);
  }
  return finish_build_schema(schema);
}

int main(int argc, const char** argv) {
  if (argc < 2) {
    printf("usage ./htap_stream_generator <ipc_socket>");
    return 1;
  }
  std::string ipc_socket = std::string(argv[1]);

  // prepare environment variable
  setenv("VINEYARD_IPC_SOCKET", ipc_socket.c_str(), 1 /* overwrite */);
  LOG(INFO) << "Prepared VINEYARD_IPC_SOCKET env: " << ipc_socket;

  ::ObjectID global_builder_id, builder_id;
  uint64_t instance_id;
  {
    auto schema = MakeSchema();
    auto g = create_graph_builder("test_graph", schema, 0);
    get_builder_id(g, &builder_id, &instance_id);
    LOG(INFO) << "builder id: " << builder_id
              << ", instance_id = " << instance_id;
  }

  {
    global_builder_id =
        build_global_graph_stream("test_graph", 1, &builder_id, &instance_id);
    LOG(INFO) << "global builder id: " << global_builder_id;
  }

  auto g = get_graph_builder("test_graph", 0);
  VINEYARD_ASSERT(g != nullptr);

  {
    int32_t x1 = 1;
    int64_t x2 = 2;
    char str[] = "abcde";
    Property props[] = {
        {
            1,
            PropertyType::LONG,
            nullptr,
            0,
        },
        {
            2,
            PropertyType::STRING,
            str,
            5,
        },
        {3, PropertyType::INT, nullptr, 0},
    };
    {
      vineyard::htap_types::PodProperties pp;
      pp.long_value = x2;
      props[0].len = pp.long_value;
    }
    {
      vineyard::htap_types::PodProperties pp;
      pp.int_value = x1;
      props[2].len = pp.long_value;
    }
    for (int i = 0; i < 10; ++i) {
      add_vertex(g, i, 1, 3, props);
    }
  }

  {
    int64_t x2 = 2;
    char str[] = "abcde";
    Property props[] = {
        {1, PropertyType::LONG, nullptr, 0},
        {2, PropertyType::STRING, str, 5},
        {4, PropertyType::STRING, str, 5},
        {5, PropertyType::STRING, str, 5},
    };
    {
      vineyard::htap_types::PodProperties pp;
      pp.long_value = x2;
      props[0].len = pp.long_value;
    }
    for (int i = 10; i < 20; ++i) {
      add_vertex(g, i, 2, 4, props);
    }
  }

  {
    double x3 = 3.0;
    Property props[] = {
        {6, PropertyType::DOUBLE, nullptr, 0},
    };
    {
      vineyard::htap_types::PodProperties pp;
      pp.double_value = x3;
      props[0].len = pp.long_value;
    }
    for (int i = 0; i < 10; ++i) {
      add_edge(g, i, i, i, 7 /* label */, 1, 1, 1, props);
    }
  }

  {
    double x3 = 3.0;
    Property props[] = {
        {6, PropertyType::DOUBLE, nullptr, 0},
    };
    {
      vineyard::htap_types::PodProperties pp;
      pp.double_value = x3;
      props[0].len = pp.long_value;
    }
    for (int i = 0; i < 10; ++i) {
      add_edge(g, i, i, i + 10, 8 /* label */, 1, 2, 1, props);
    }
  }

  build(g);
  destroy(g);

  LOG(INFO) << "producer generates all vertex/edge chunks";
  return 0;
}
