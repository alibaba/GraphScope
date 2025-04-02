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

#ifndef FLEX_ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_UTILS_H_
#define FLEX_ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_UTILS_H_
#ifdef BUILD_KAFKA_WAL_WRITER_PARSER
#include <optional>
#include <string>
#include <vector>
#include "cppkafka/cppkafka.h"
#include "flex/utils/app_utils.h"

namespace gs {
/*
 * Get all partitions of the given topic.
 */
std::vector<cppkafka::TopicPartition> get_all_topic_partitions(
    const cppkafka::Configuration& config, const std::string& topic_name,
    bool from_beginning = true);

std::optional<std::vector<char>> parse_uri(const std::string& wal_uri);

}  // namespace gs
#endif

#endif  // FLEX_ENGINES_GRAPH_DB_DATABASE_WAL_KAFKA_WAL_UTILS_H_