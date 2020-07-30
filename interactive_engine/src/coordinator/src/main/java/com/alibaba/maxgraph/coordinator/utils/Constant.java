/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.coordinator.utils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class Constant {

    private Constant() {

    }

    public static final Charset DEFAULT_STRING_ENDOCE = StandardCharsets.UTF_8;

    public static final String COORDINATOR = "Coordinator";
    public static final String SERVICE_NAME = "/service_name";
    public static final String SERVER_ADDRESS = "/servers";

    public static final String SCHEMA = "/schema";
    public static final String SERVER_HEARTBEAT = "/server_heartbeat";
    public static final String ONLINE_PARTITION = "/online";
    public static final String RUNTIME_ADDR = "/runtime_addr";

    public static final byte[] SERVICE_NAME_BYTES = SERVICE_NAME.getBytes(DEFAULT_STRING_ENDOCE);
    public static final byte[] SERVER_ADDRESS_BYTES = SERVER_ADDRESS.getBytes(DEFAULT_STRING_ENDOCE);
    public static final byte[] SCHEMA_BYTES = SCHEMA.getBytes(DEFAULT_STRING_ENDOCE);
    public static final byte[] SERVER_HEARTBEAT_BYTES = SERVER_HEARTBEAT.getBytes(DEFAULT_STRING_ENDOCE);
    public static final byte[] ONLINE_PARTITION_BYTES = ONLINE_PARTITION.getBytes(DEFAULT_STRING_ENDOCE);
    public static final byte[] RUNTIME_ADDR_BYTES = RUNTIME_ADDR.getBytes(DEFAULT_STRING_ENDOCE);

    public static final byte[] DEFAULT_BODY = "ok".getBytes(DEFAULT_STRING_ENDOCE);

    // url

}
