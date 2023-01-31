#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    ./server.py [<port>]
"""

import ast
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import traceback


executed_code = []
global_ctx = dict()
local_ctx = dict()


class InteractiveTestServer(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super(InteractiveTestServer, self).__init__(*args, **kwargs)

    def _set_response(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write("\n".join(executed_code).encode("utf-8"))

    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        code = post_data = self.rfile.read(content_length).decode("utf-8")
        logging.info("eval: %s" % code)
        try:
            is_expression = False
            try:
                ast.parse(code, mode="eval")
                is_expression = True
            except:
                pass
            if is_expression:
                r = eval(code, global_ctx, local_ctx)
            else:
                r = exec(code, global_ctx, local_ctx)
            executed_code.append(code)
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(("%s" % r).encode("utf-8"))
        except Exception as e:
            print(e, traceback.format_exc())
            self.send_response(500)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(("%s" % e).encode("utf-8"))
        logging.info("eval done")


def run(server_class=HTTPServer, handler_class=InteractiveTestServer, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    logging.info("Starting testing server on %d..." % port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        for k, v in local_ctx.items():
            try:
                del v
            except:
                pass
        for k, v in global_ctx.items():
            try:
                del v
            except:
                pass
    httpd.server_close()
    logging.info("Stopping testing server ...")


if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
