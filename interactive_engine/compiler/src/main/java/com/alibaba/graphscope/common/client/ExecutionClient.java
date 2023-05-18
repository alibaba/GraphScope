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

package com.alibaba.graphscope.common.client;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.ir.runtime.type.PhysicalPlan;
import com.alibaba.graphscope.gaia.proto.IrResult;

import java.util.Iterator;

public abstract class ExecutionClient<C> {
    protected final ChannelFetcher<C> channelFetcher;
    public ExecutionClient(ChannelFetcher<C> channelFetcher) {
        this.channelFetcher = channelFetcher;
    }
    public abstract Iterator<IrResult.Record> submit(Request request) throws Exception;

    public abstract void close() throws Exception;

    public static class Request {
        private final long requestId;
        private final String requestName;
        private final PhysicalPlan requestPlan;

        public Request(long requestId, String requestName, PhysicalPlan requestPlan) {
            this.requestId = requestId;
            this.requestName = requestName;
            this.requestPlan = requestPlan;
        }

        public long getRequestId() {
            return requestId;
        }

        public String getRequestName() {
            return requestName;
        }

        public PhysicalPlan getRequestPlan() {
            return requestPlan;
        }
    }
}
