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

#ifndef FLEX_INTERACTIVE_EXAMPLES_LDBC_PLUGINS_INTERACTIVE_UTILS_H_
#define FLEX_INTERACTIVE_EXAMPLES_LDBC_PLUGINS_INTERACTIVE_UTILS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/hqps_db/core/sync_engine.h"

namespace gs {
void encode_ic1_result(const results::CollectiveResults& ic1_result,
                       Encoder& encoder) {
  auto size = ic1_result.results_size();
  for (int32_t i = 0; i < size; ++i) {
    auto& result = ic1_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i32();
    const auto& personLastName =
        result.record().columns(0).entry().element().object().str();
    const auto& person_distance =
        result.record().columns(0).entry().element().object().i32();
    LOG(INFO) << "personId: " << personId
              << " personLastName: " << personLastName
              << " person_distance: " << person_distance;

    LOG(INFO) << result.DebugString();
  }
}

void encode_ic2_result(const results::CollectiveResults& ic2_result,
                       Encoder& encoder) {
  LOG(INFO) << "encode_ic2_result: " << ic2_result.DebugString();

  auto size = ic2_result.results_size();
  for (int32_t i = 0; i < size; ++i) {
    auto& result = ic2_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i64();
    const auto& personFirstName =
        result.record().columns(1).entry().element().object().str();
    const auto& personLastName =
        result.record().columns(2).entry().element().object().str();
    const auto& messageId =
        result.record().columns(3).entry().element().object().i64();
    const auto& messageContent =
        result.record().columns(4).entry().element().object().str();
    const auto& messageImageFile =
        result.record().columns(5).entry().element().object().str();
    const auto& messageCreationDate =
        result.record().columns(6).entry().element().object().i64();
    // auto& result = ic1_result.results(i);
    // const auto& personId =
    //     result.record().columns(0).entry().element().object().i32();
    // const auto& personLastName =
    //     result.record().columns(0).entry().element().object().str();
    // const auto& person_distance =
    //     result.record().columns(0).entry().element().object().i32();
    // LOG(INFO) << "personId: " << personId
    //           << " personLastName: " << personLastName
    //           << " person_distance: " << person_distance;
    encoder.put_long(personId);
    encoder.put_string_view(personFirstName);
    encoder.put_string_view(personLastName);
    encoder.put_long(messageId);
    if (messageContent.empty()) {
      encoder.put_string_view(messageImageFile);
    } else {
      encoder.put_string_view(messageContent);
    }
    encoder.put_long(messageCreationDate);

    LOG(INFO) << result.DebugString();
  }
}

void encode_ic3_result(const results::CollectiveResults& ic3_result,
                       Encoder& encoder) {
  LOG(INFO) << "encode_ic3_result: " << ic3_result.DebugString();
  for (int32_t i = 0; i < ic3_result.results_size(); ++i) {
    auto& result = ic3_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i64();
    const auto& personFirstName =
        result.record().columns(1).entry().element().object().str();
    const auto& personLastName =
        result.record().columns(2).entry().element().object().str();
    const auto& x_count =
        result.record().columns(3).entry().element().object().i64();
    const auto& y_count =
        result.record().columns(4).entry().element().object().i64();
    const auto& total_cnt =
        result.record().columns(5).entry().element().object().i64();
    encoder.put_long(personId);
    encoder.put_string_view(personFirstName);
    encoder.put_string_view(personLastName);
    encoder.put_long(x_count);
    encoder.put_long(y_count);
    encoder.put_long(total_cnt);
  }
}

void encode_ic4_result(const results::CollectiveResults& ic4_result,
                       Encoder& encoder) {
  LOG(INFO) << "encode_ic4_result: " << ic4_result.DebugString();
  for (int32_t i = 0; i < ic4_result.results_size(); ++i) {
    auto& result = ic4_result.results(i);
    const auto& tagName =
        result.record().columns(0).entry().element().object().str();
    const auto& postCount =
        result.record().columns(1).entry().element().object().i32();
    encoder.put_string_view(tagName);
    encoder.put_int(postCount);
  }
}

void encode_ic6_result(const results::CollectiveResults& ic6_result,
                       Encoder& encoder) {
  LOG(INFO) << "encode_ic6_result: " << ic6_result.DebugString();
  for (int32_t i = 0; i < ic6_result.results_size(); ++i) {
    auto& result = ic6_result.results(i);
    const auto& tagName =
        result.record().columns(0).entry().element().object().str();
    const auto& postCount =
        result.record().columns(1).entry().element().object().i64();
    encoder.put_string_view(tagName);
    encoder.put_int(postCount);
  }
}

void encode_ic8_result(const results::CollectiveResults& ic8_result,
                       Encoder& encoder) {
  LOG(INFO) << "encode_ic8_result: " << ic8_result.DebugString();
  for (int32_t i = 0; i < ic8_result.results_size(); ++i) {
    auto& result = ic8_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i64();
    const auto& personFirstName =
        result.record().columns(1).entry().element().object().str();
    const auto& personLastName =
        result.record().columns(2).entry().element().object().str();
    const auto& creationDate =
        result.record().columns(3).entry().element().object().i64();
    const auto& commentId =
        result.record().columns(4).entry().element().object().i64();
    const auto& commentContent =
        result.record().columns(5).entry().element().object().str();
    encoder.put_long(personId);
    encoder.put_string_view(personFirstName);
    encoder.put_string_view(personLastName);
    encoder.put_long(creationDate);
    encoder.put_long(commentId);
    encoder.put_string_view(commentContent);
  }
}

void encode_ic9_result(const results::CollectiveResults& ic9_result,
                       Encoder& encoder) {
  LOG(INFO) << "encode_ic9_result: " << ic9_result.DebugString();
  for (int32_t i = 0; i < ic9_result.results_size(); ++i) {
    auto& result = ic9_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i64();
    const auto& personFirstName =
        result.record().columns(1).entry().element().object().str();
    const auto& personLastName =
        result.record().columns(2).entry().element().object().str();
    const auto& messageId =
        result.record().columns(3).entry().element().object().i64();
    const auto& messageContent =
        result.record().columns(4).entry().element().object().str();
    const auto& messageImageFile =
        result.record().columns(5).entry().element().object().str();
    const auto& messageCreationDate =
        result.record().columns(6).entry().element().object().i64();
    encoder.put_long(personId);
    encoder.put_string_view(personFirstName);
    encoder.put_string_view(personLastName);
    encoder.put_long(messageId);
    if (messageContent.empty()) {
      encoder.put_string_view(messageImageFile);
    } else {
      encoder.put_string_view(messageContent);
    }
    encoder.put_long(messageCreationDate);
  }
}

void encode_ic10_result(const results::CollectiveResults& ic10_result,
                        Encoder& encoder) {
  LOG(INFO) << "encode_ic10_result: " << ic10_result.DebugString();
  for (int32_t i = 0; i < ic10_result.results_size(); ++i) {
    auto& result = ic10_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i64();
    const auto& personFirstName =
        result.record().columns(1).entry().element().object().str();
    const auto& personLastName =
        result.record().columns(2).entry().element().object().str();
    const auto& score =
        result.record().columns(3).entry().element().object().i32();
    const auto& personGender =
        result.record().columns(4).entry().element().object().str();
    const auto& cityName =
        result.record().columns(5).entry().element().object().str();
    encoder.put_long(personId);
    encoder.put_string_view(personFirstName);
    encoder.put_string_view(personLastName);
    encoder.put_int(score);
    encoder.put_string_view(personGender);
    encoder.put_string_view(cityName);
  }
}

void encode_ic11_result(const results::CollectiveResults& ic11_result,
                        Encoder& encoder) {
  LOG(INFO) << "encode_ic11_result: " << ic11_result.DebugString();
  for (int32_t i = 0; i < ic11_result.results_size(); ++i) {
    auto& result = ic11_result.results(i);
    const auto& personId =
        result.record().columns(0).entry().element().object().i64();
    const auto& personFirstName =
        result.record().columns(1).entry().element().object().str();
    const auto& personLastName =
        result.record().columns(2).entry().element().object().str();
    const auto& companyName =
        result.record().columns(3).entry().element().object().str();
    const auto& workFromYear =
        result.record().columns(4).entry().element().object().i32();
    encoder.put_long(personId);
    encoder.put_string_view(personFirstName);
    encoder.put_string_view(personLastName);
    encoder.put_string_view(companyName);
    encoder.put_int(workFromYear);
  }
}

void encode_ic12_result(const results::CollectiveResults& ic12_result,
                        Encoder& encoder) {
  LOG(INFO) << "encode_ic12_result: " << ic12_result.DebugString();
  for (int32_t i = 0; i < ic12_result.results_size(); ++i) {
    auto& result = ic12_result.results(i);
    // const auto& personId =
    //     result.record().columns(0).entry().element().object().i64();
    // const auto& personFirstName =
    //     result.record().columns(1).entry().element().object().str();
    // const auto& personLastName =
    //     result.record().columns(2).entry().element().object().str();
    // const auto& tagNames =
    //     result.record().columns(3).entry().element().object().str();
    // const auto& replyCount =
    //     result.record().columns(4).entry().element().object().i32();
    // encoder.put_long(personId);
    // encoder.put_string_view(personFirstName);
    // encoder.put_string_view(personLastName);
    // encoder.put_string_view(tagNames);
    // encoder.put_int(replyCount);
  }
}

}  // namespace gs

#endif  // FLEX_INTERACTIVE_EXAMPLES_LDBC_PLUGINS_INTERACTIVE_UTILS_H_