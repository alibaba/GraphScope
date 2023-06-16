#ifndef CORE_HTTP_HANDLER_H_
#define CORE_HTTP_HANDLER_H_

#include <seastar/http/httpd.hh>

namespace snb::ic {

class http_handler {
public:
  http_handler(uint16_t http_port);

  void start();
  void stop();

private:
  seastar::future<> set_routes();

private:
  const uint16_t http_port_;
  seastar::httpd::http_server_control server_;
};

}  // namespace snb::ic

#endif  // CORE_HTTP_HANDLER_H_