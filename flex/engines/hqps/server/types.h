#ifndef CORE_TYPES_ACT_H_
#define CORE_TYPES_ACT_H_

#include <hiactor/net/serializable_queue.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include <string>

namespace snb::ic {

using timestamp_t = uint32_t;

template <typename BufType,
          typename = std::enable_if_t<
              std::is_nothrow_move_constructible<BufType>::value &&
                  std::is_nothrow_move_assignable<BufType>::value &&
                  std::is_nothrow_destructible<BufType>::value,
              void>>
struct payload {
  explicit payload(BufType&& content) : content(std::move(content)) {}
  ~payload() = default;

  payload(const payload&) = delete;
  payload& operator=(const payload&) = delete;
  payload(payload&&) = default;
  payload& operator=(payload&&) = default;

  void dump_to(hiactor::serializable_queue& qu) {}

  static payload load_from(hiactor::serializable_queue& qu) {
    return payload{BufType{}};
  }

  BufType content;
};

using query_param = payload<seastar::sstring>;
using query_result = payload<seastar::sstring>;

}  // namespace snb::ic

#endif  // CORE_TYPES_ACT_H_