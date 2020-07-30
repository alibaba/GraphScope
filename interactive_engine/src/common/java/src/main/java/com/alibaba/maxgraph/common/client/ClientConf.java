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
package com.alibaba.maxgraph.common.client;

import java.util.Map;
import java.util.Properties;

import com.alibaba.maxgraph.common.cluster.MaxGraphConfiguration;

public class ClientConf extends MaxGraphConfiguration {
    public static final String CLIENT_MAX_TRY_TIMES = "client.refresh.max.retry.times";
    public static final String CLIENT_REFRESH_BACKGROUND_INTERVAL_SEC = "client.refresh.background.interval.sec";
    public static final String CLIENT_DATA_ORDER_PRESERVING_ENABLED = "client.data.order.preserving.enable";
    public static final String CLIENT_SEND_DATA_RETRY_TIMES = "client.send.data.retry.times";
    public static final String FRONTEND_SERVICE_ADDRESS = "frontendservice.ip";
    public static final String FRONTENDSERVICE_PORT = "frontendservice.port";
    public static final String CLIENT_NAME = "client.name";

    public ClientConf(Map<String, String> conf) {
        super(conf);
    }

    public ClientConf(Properties properties) {
        super(properties);
    }

    public int getClientMaxTryNum() {
        return getInt(CLIENT_MAX_TRY_TIMES, 3);
    }

    public int getClientRefreshBackgroundIntervalSec() {
        return getInt(CLIENT_REFRESH_BACKGROUND_INTERVAL_SEC, 300);
    }

    public boolean isOrderPreservingEnable() {
        return getBoolean(CLIENT_DATA_ORDER_PRESERVING_ENABLED, true);
    }

    public int getClientSendDataRetryTimes() {
        return getInt(CLIENT_SEND_DATA_RETRY_TIMES, 3);
    }

    public String getFrontendServiceAddress() {
        return getString(FRONTEND_SERVICE_ADDRESS);
    }

    public int getFrontendServicePort() {
        return getInt(FRONTENDSERVICE_PORT);
    }

    public String getClientName() {
        return getString(CLIENT_NAME, "maxgraph");
    }
}
