/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.config;

public class HQPSConfig {
    public static final Config<String> HQPS_URIS =
            Config.stringConfig("hqps.uris", "http://localhost:8080");

    public static final Config<Long> HQPS_HTTP_TIMEOUT =
            Config.longConfig("hqps.http.timeout", 6000000l);
}
