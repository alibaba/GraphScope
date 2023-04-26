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

#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/third_party/httplib.h"

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "usage: rt_admin <operation> [args...]" << std::endl;
    return -1;
  }

  std::string url;
  const char* url_env = std::getenv("GRAPHSCOPE_IP");
  if (url_env == NULL) {
    url = "127.0.0.1";
  } else {
    url = url_env;
  }
  const char* port_env = std::getenv("GRAPHSCOPE_PORT");
  int port = 10000;
  if (port_env != NULL) {
    port = atoi(port_env);
  }
  httplib::Client cli(url, port);

  std::string op = argv[1];
  if (op == "--version" || op == "-v") {
    std::cout << "GraphScope/Flex version " << FLEX_VERSION << std::endl;
    return 0;
  }
  std::string label_name, src_label, dst_label, edge_label;
  int64_t vertex_id, src_id, dst_id;
  for (auto& c : op) {
    c = toupper(c);
  }

  std::vector<char> buf;
  gs::Encoder encoder(buf);
  if (op == "SHOW_STORED_PROCEDURES") {
    encoder.put_string(op);
    encoder.put_byte(0);
  } else if (op == "QUERY_VERTEX") {
    if (argc < 4) {
      std::cerr << "usage for vertex query: rt_admin query_vertex "
                   "<vertex-label> <vertex-id>"
                << std::endl;
      return -1;
    }
    label_name = argv[2];
    vertex_id = atol(argv[3]);
    encoder.put_string(op);
    encoder.put_string(label_name);
    encoder.put_long(vertex_id);
    encoder.put_byte(0);
  } else if (op == "QUERY_EDGE") {
    if (argc < 7) {
      std::cerr << "usage for edge query: rt_admin query_edge <src-label> "
                   "<src-id> <dst-label> <dst-id> <edge-label>"
                << std::endl;
      return -1;
    }
    src_label = argv[2];
    std::string src_id_str = argv[3];
    if (src_id_str == "_ANY_ID") {
      src_id = std::numeric_limits<int64_t>::max();
    } else {
      src_id = atol(argv[3]);
    }
    dst_label = argv[4];
    std::string dst_id_str = argv[5];
    if (dst_id_str == "_ANY_ID") {
      dst_id = std::numeric_limits<int64_t>::max();
    } else {
      dst_id = atol(argv[5]);
    }
    edge_label = argv[6];
    encoder.put_string(op);
    encoder.put_string(src_label);
    encoder.put_long(src_id);
    encoder.put_string(dst_label);
    encoder.put_long(dst_id);
    encoder.put_string(edge_label);
    encoder.put_byte(0);
  } else {
    std::cerr << "unexpected op - " << op << std::endl;
    return -1;
  }

  std::string content(buf.data(), buf.size());
  auto res = cli.Post("/interactive/app", content, "text/plain");

  std::string ret = res->body;
  if (op == "SHOW_STORED_PROCEDURES") {
    if (!ret.empty()) {
      gs::Decoder decoder(ret.data(), ret.size());
      int index = 1;
      while (!decoder.empty()) {
        std::string_view cur = decoder.get_string();
        if (!cur.empty()) {
          std::cout << "[app-" << index << "]: " << cur << std::endl;
        }
        index++;
      }
    }
  } else if (op == "QUERY_VERTEX") {
    gs::Decoder decoder(ret.data(), ret.size());
    if (decoder.empty()) {
      std::cerr << "Query vertex - " << label_name << " - " << vertex_id
                << " failed..." << std::endl;
    }
    int exist = decoder.get_int();
    if (exist == 0) {
      std::cout << "Vertex - " << label_name << " - " << vertex_id
                << " not found..." << std::endl;
    } else {
      std::cout << "Vertex - " << label_name << " - " << vertex_id
                << " found, properties: " << std::endl;
      while (!decoder.empty()) {
        std::string_view cur = decoder.get_string();
        std::cout << "\t" << cur << std::endl;
      }
    }
  } else if (op == "QUERY_EDGE") {
    gs::Decoder decoder(ret.data(), ret.size());
    if (decoder.empty()) {
      std::cerr << "Query failed..." << std::endl;
    }
    int status = decoder.get_int();
    if (status == 0) {
      std::cout << "No edge found..." << std::endl;
    } else if (status == 1) {
      while (!decoder.empty()) {
        std::string_view src_label = decoder.get_string();
        std::string_view dst_label = decoder.get_string();
        std::string_view edge_label = decoder.get_string();

        std::cout << src_label << " - " << edge_label << " - " << dst_label
                  << std::endl;
        int num = decoder.get_int();
        for (int i = 0; i < num; ++i) {
          int64_t src_id = decoder.get_long();
          int64_t dst_id = decoder.get_long();
          std::string_view data = decoder.get_string();
          std::cout << "\t" << src_id << " - " << dst_id << ": " << data
                    << std::endl;
        }
      }
    } else if (status == 2) {
      std::cout << "Too many (over 1000) edges found..." << std::endl;
    } else {
      std::cerr << "Query failed..." << std::endl;
    }
  }

  return 0;
}
