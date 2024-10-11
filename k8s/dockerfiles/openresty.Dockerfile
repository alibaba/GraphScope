FROM openresty/openresty:jammy

# Copy https://github.com/ledgetech/lua-resty-http/blob/master/lib/resty/http_connect.lua to /usr/local/openresty/lualib/resty/http_connect.lua
COPY ./dockerfiles/resty/http_connect.lua /usr/local/openresty/lualib/resty/http_connect.lua
COPY ./dockerfiles/resty/http_headers.lua /usr/local/openresty/lualib/resty/http_headers.lua
COPY ./dockerfiles/resty/http.lua /usr/local/openresty/lualib/resty/http.lua