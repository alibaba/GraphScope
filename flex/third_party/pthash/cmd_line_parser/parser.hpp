/** Copyright 2019 Giulio Ermanno Pibiri
 *
 * The following sets forth attribution notices for third party software.
 *
 * Command Line Parser for C++17:
 * The software includes components licensed by Giulio Ermanno Pibiri and
 * Roberto Trani, available at https://github.com/jermp/cmd_line_parser
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <iostream>
#include <sstream>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace cmd_line_parser {

struct parser {
  inline static const std::string empty = "";

  parser(int argc, char** argv) : m_argc(argc), m_argv(argv), m_required(0) {}

  struct cmd {
    std::string shorthand, value, descr;
    bool is_boolean;
  };

  bool parse() {
    if (size_t(m_argc - 1) < m_required)
      return abort();
    size_t k = 0;
    for (int i = 1; i != m_argc; ++i, ++k) {
      std::string parsed(m_argv[i]);
      if (parsed == "-h" or parsed == "--help")
        return abort();
      size_t id = k;
      bool is_optional = id >= m_required;
      if (is_optional) {
        auto it = m_shorthands.find(parsed);
        if (it == m_shorthands.end()) {
          std::cerr << "== error: shorthand '" + parsed + "' not found"
                    << std::endl;
          return abort();
        }
        id = (*it).second;
      }
      assert(id < m_names.size());
      auto const& name = m_names[id];
      auto& c = m_cmds[name];
      if (is_optional) {
        if (c.is_boolean) {
          parsed = "true";
        } else {
          ++i;
          if (i == m_argc)
            return abort();
          parsed = m_argv[i];
        }
      }
      c.value = parsed;
    }
    return true;
  }

  void help() const {
    std::cerr << "Usage: " << m_argv[0] << " [-h,--help]";
    auto print = [this](bool with_description) {
      for (size_t i = 0; i != m_names.size(); ++i) {
        auto const& c = m_cmds.at(m_names[i]);
        bool is_optional = i >= m_required;
        if (is_optional)
          std::cerr << " [" << c.shorthand;
        if (!c.is_boolean)
          std::cerr << " " << m_names[i];
        if (is_optional)
          std::cerr << "]";
        if (with_description)
          std::cerr << "\n\t" << c.descr << "\n\n";
      }
    };
    print(false);
    std::cerr << "\n\n";
    print(true);
    std::cerr << " [-h,--help]\n\tPrint this help text and silently exits."
              << std::endl;
  }

  bool add(std::string const& name, std::string const& descr) {
    bool ret = m_cmds.emplace(name, cmd{empty, empty, descr, false}).second;
    if (ret) {
      m_names.push_back(name);
      m_required += 1;
    }
    return ret;
  }

  bool add(std::string const& name, std::string const& descr,
           std::string const& shorthand, bool is_boolean = true) {
    bool ret = m_cmds
                   .emplace(name, cmd{shorthand, is_boolean ? "false" : empty,
                                      descr, is_boolean})
                   .second;
    if (ret) {
      m_names.push_back(name);
      m_shorthands.emplace(shorthand, m_names.size() - 1);
    }
    return ret;
  }

  template <typename T>
  T get(std::string const& name) const {
    auto it = m_cmds.find(name);
    if (it == m_cmds.end())
      throw std::runtime_error("error: '" + name + "' not found");
    auto const& value = (*it).second.value;
    return parse<T>(value);
  }

  bool parsed(std::string const& name) const {
    auto it = m_cmds.find(name);
    if (it == m_cmds.end() or (*it).second.value == empty)
      return false;
    return true;
  }

  template <typename T>
  T parse(std::string const& value) const {
    if constexpr (std::is_same<T, std::string>::value) {
      return value;
    } else if constexpr (std::is_same<T, char>::value or
                         std::is_same<T, signed char>::value or
                         std::is_same<T, unsigned char>::value) {
      return value.front();
    } else if constexpr (std::is_same<T, unsigned int>::value or
                         std::is_same<T, int>::value or
                         std::is_same<T, unsigned short int>::value or
                         std::is_same<T, short int>::value) {
      return std::atoi(value.c_str());
    } else if constexpr (std::is_same<T, unsigned long int>::value or
                         std::is_same<T, long int>::value or
                         std::is_same<T, unsigned long long int>::value or
                         std::is_same<T, long long int>::value) {
      return std::atoll(value.c_str());
    } else if constexpr (std::is_same<T, float>::value or
                         std::is_same<T, double>::value or
                         std::is_same<T, long double>::value) {
      return std::atof(value.c_str());
    } else if constexpr (std::is_same<T, bool>::value) {
      std::istringstream stream(value);
      bool ret;
      if (value == "true" or value == "false") {
        stream >> std::boolalpha >> ret;
      } else {
        stream >> std::noboolalpha >> ret;
      }
      return ret;
    }
    assert(false);  // should never happen
    throw std::runtime_error("unsupported type");
  }

 private:
  int m_argc;
  char** m_argv;
  size_t m_required;
  std::unordered_map<std::string, cmd> m_cmds;
  std::unordered_map<std::string, int> m_shorthands;
  std::vector<std::string> m_names;

  bool abort() const {
    help();
    return false;
  }
};

}  // namespace cmd_line_parser
