/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.client;

import com.alibaba.graphscope.gremlin.plugin.QueryLogger;

import io.grpc.*;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

public class RpcInterceptor implements ClientInterceptor {
    public static final CallOptions.Key<QueryLogger> QUERY_LOGGER_OPTION =
            CallOptions.Key.create("query-logger");

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> methodDescriptor,
            CallOptions callOptions,
            Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                channel.newCall(methodDescriptor, callOptions)) {
            private Instant requestStartTime;

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                requestStartTime = Instant.now();
                QueryLogger queryLogger = callOptions.getOption(QUERY_LOGGER_OPTION);
                if (queryLogger != null) {
                    queryLogger.info(
                            "[query][submitted]: submit the query to the channel {}",
                            channel.authority());
                }
                super.start(
                        new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                                responseListener) {
                            private final AtomicBoolean firstResponseLogged =
                                    new AtomicBoolean(false);

                            @Override
                            public void onMessage(RespT message) {
                                if (firstResponseLogged.compareAndSet(false, true)) {
                                    long firstResponseTime =
                                            Instant.now().toEpochMilli()
                                                    - requestStartTime.toEpochMilli();
                                    if (queryLogger != null) {
                                        queryLogger.info(
                                                "[query][response]: receive the first response from"
                                                        + " the channel {} in {} ms",
                                                channel.authority(),
                                                firstResponseTime);
                                    }
                                }
                                super.onMessage(message);
                            }

                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                long endTime = Instant.now().toEpochMilli();
                                long totalTime = endTime - requestStartTime.toEpochMilli();
                                if (queryLogger != null) {
                                    queryLogger.info(
                                            "[query][response]: receive the last response from the"
                                                    + " channel {} with status {} in {} ms",
                                            channel.authority(),
                                            status,
                                            totalTime);
                                }
                                super.onClose(status, trailers);
                            }
                        },
                        headers);
            }
        };
    }
}
