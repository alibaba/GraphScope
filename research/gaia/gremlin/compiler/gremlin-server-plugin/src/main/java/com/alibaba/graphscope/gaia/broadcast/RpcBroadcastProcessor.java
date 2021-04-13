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
package com.alibaba.graphscope.gaia.broadcast;

import com.alibaba.graphscope.gaia.result.GremlinResultProcessor;
import com.alibaba.pegasus.RpcChannel;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.CloseableIterator;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import io.grpc.Status;
import org.apache.tinkerpop.gremlin.server.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RpcBroadcastProcessor extends AbstractBroadcastProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RpcBroadcastProcessor.class);
    private RpcClient rpcClient;

    public RpcBroadcastProcessor(List<String> hosts) {
        super(hosts);
        List<RpcChannel> rpcChannels = new ArrayList<>();
        hostAddresses.forEach(k -> {
            String host = k.getLeft();
            int port = k.getRight();
            rpcChannels.add(new RpcChannel(host, port));
        });
        rpcClient = new RpcClient(rpcChannels);
    }

    @Override
    public void broadcast(PegasusClient.JobRequest request, Context writeResult) {
        CloseableIterator<PegasusClient.JobResponse> iterator = null;
        ResultProcessor processor = new GremlinResultProcessor(writeResult);
        try {
            iterator = rpcClient.submit(request);
            // process response
            while (iterator.hasNext()) {
                PegasusClient.JobResponse response = iterator.next();
                processor.process(response);
            }
            processor.finish();
        } catch (Exception e) {
            logger.error("broadcast exception {}", e);
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
        logger.debug("start to close rpc client");
        this.rpcClient.shutdown();
    }
}
