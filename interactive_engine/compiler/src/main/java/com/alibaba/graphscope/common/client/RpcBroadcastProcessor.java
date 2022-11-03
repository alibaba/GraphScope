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

import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.CloseableIterator;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;

import io.grpc.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RpcBroadcastProcessor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RpcBroadcastProcessor.class);

    protected RpcClient rpcClient;
    protected RpcChannelFetcher fetcher;

    public RpcBroadcastProcessor(RpcChannelFetcher fetcher) {
        this.fetcher = fetcher;
        if (!fetcher.isDynamic()) {
            this.rpcClient = new RpcClient(fetcher.fetch());
        }
    }

    public void broadcast(PegasusClient.JobRequest request, ResultProcessor processor) {
        CloseableIterator<PegasusClient.JobResponse> iterator = null;
        try {
            if (fetcher.isDynamic()) {
                this.rpcClient = new RpcClient(fetcher.fetch());
            }
            iterator = rpcClient.submit(request);
            // process response
            while (iterator.hasNext()) {
                PegasusClient.JobResponse response = iterator.next();
                processor.process(response);
            }
            processor.finish();
        } catch (Throwable e) {
            logger.error("get result from grpc returns error {}", e);
            processor.error(Status.fromThrowable(e));
        } finally {
            if (iterator != null) {
                try {
                    iterator.close();
                } catch (IOException ioe) {
                    logger.error("iterator close fail {}", ioe);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.rpcClient.shutdown();
    }
}
