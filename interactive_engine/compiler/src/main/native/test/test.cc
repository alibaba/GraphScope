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
#include "graph_planner.h"

#include <fstream>
#include <sstream>

#include <sys/stat.h>

std::string get_dir_name()
{
    // Get the directory of this source file
    std::string current_dir = __FILE__;
    size_t pos = current_dir.find_last_of("/");
    current_dir = current_dir.substr(0, pos);
    return current_dir;
}

void check_path_exits(const std::string &path)
{
    // split path by ':'
    std::vector<std::string> paths;
    std::string::size_type start = 0;
    std::string::size_type end = path.find(':');
    while (end != std::string::npos)
    {
        auto sub_path = path.substr(start, end - start);
        paths.push_back(sub_path);
        start = end + 1;
        end = path.find(':', start);
    }
    auto sub_path = path.substr(start);
    paths.push_back(sub_path);

    for (const auto &p : paths)
    {
        struct stat buffer;
        if (stat(p.c_str(), &buffer) != 0)
        {
            std::cerr << "Path not exists: " << p << std::endl;
            exit(1);
        }
    }
    std::cout << "Path exists: " << path << std::endl;
}

std::string read_string_from_file(const std::string &file_path)
{
    std::ifstream inputFile(file_path); // Open the file for reading

    if (!inputFile.is_open())
    {
        std::cerr << "Error: Could not open the file " << file_path << std::endl;
        exit(1);
    }
    // Use a stringstream to read the entire content of the file
    std::ostringstream buffer;
    buffer << inputFile.rdbuf(); // Read the file stream into the stringstream

    std::string fileContent = buffer.str(); // Get the string from the stringstream
    inputFile.close();                      // Close the file

    return fileContent;
}

int main(int argc, char **argv)
{
    // Check if the correct number of arguments is provided
    if (argc != 7)
    {
        std::cerr << "Usage: " << argv[0]
                  << " <java class path> <jna lib path> <graph schema path> <graph statistics path> <query> <config path>" << std::endl;
        return 1;
    }

    std::string java_class_path = argv[1];
    std::string jna_class_path = argv[2];
    std::string graph_schema_path = argv[3];
    std::string graph_statistic_path = argv[4];

    // check director or file exists
    check_path_exits(java_class_path);
    check_path_exits(jna_class_path);
    check_path_exits(graph_schema_path);
    check_path_exits(graph_statistic_path);

    gs::GraphPlannerWrapper graph_planner_wrapper(
        java_class_path, jna_class_path);

    std::string schema_content = read_string_from_file(graph_schema_path);
    std::string statistic_content = read_string_from_file(graph_statistic_path);

    std::string cypher_query_string = argv[5];
    std::string config_path = argv[6];
    auto plan =
        graph_planner_wrapper.CompilePlan(config_path, cypher_query_string, schema_content, statistic_content);
    std::cout << "Plan: " << plan.physical_plan.DebugString() << std::endl;
    std::cout << "schema: " << plan.result_schema << std::endl;
    return 0;
}