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
package com.alibaba.maxgraph.common.rpc;

public class RpcConfig {
    private static final String QUERY_TIMEOUT_SEC = "query.timeout.sec";

    private long queryTimeoutSec;
    private int channelCount;

    public RpcConfig() {
        this.queryTimeoutSec = 600;
        this.channelCount = 32;
    }

    public long getQueryTimeoutSec() {
        return queryTimeoutSec;
    }

    public void setQueryTimeoutSec(long queryTimeoutSec) {
        this.queryTimeoutSec = queryTimeoutSec;
    }

    public void setChannelCount(int channelCount) {
        this.channelCount = channelCount;
    }

    public int getChannelCount() {
        return this.channelCount;
    }
}
