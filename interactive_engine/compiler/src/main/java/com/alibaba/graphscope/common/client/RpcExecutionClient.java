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
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.RpcChannel;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.common.StreamIterator;
import com.alibaba.pegasus.service.protocol.PegasusClient;

import java.util.Iterator;
import java.util.function.Function;

public class RpcExecutionClient extends ExecutionClient<RpcChannel> {
    private final Configs graphConfig;
    private final RpcClient rpcClient;

    public RpcExecutionClient(Configs graphConfig, ChannelFetcher<RpcChannel> channelFetcher) {
        super(channelFetcher);
        this.graphConfig = graphConfig;
        this.rpcClient = new RpcClient(channelFetcher.fetch());
    }

    @Override
    public Iterator<IrResult.Record> submit(Request request) throws Exception {
        PegasusClient.JobRequest jobRequest =
                PegasusClient.JobRequest.parseFrom((byte[])request.getRequestPlan().build());
        PegasusClient.JobConfig jobConfig =
                PegasusClient.JobConfig.newBuilder()
                        .setJobId(request.getRequestId())
                        .setJobName(request.getRequestName())
                        .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(graphConfig))
                        .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(graphConfig))
                        .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(graphConfig))
                        .setBatchCapacity(
                                PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(graphConfig))
                        .setTimeLimit(PegasusConfig.PEGASUS_TIMEOUT.get(graphConfig))
                        .setAll(com.alibaba.pegasus.service.protocol.PegasusClient.Empty.newBuilder().build())
                        .build();
        jobRequest = jobRequest.toBuilder().setConf(jobConfig).build();
        return transform(this.rpcClient.submit(jobRequest), (PegasusClient.JobResponse response) -> {
            try {
                return IrResult.Results.parseFrom(response.getResp()).getRecord();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws Exception{
        if (rpcClient != null) {
            this.rpcClient.shutdown();
        }
    }

    private <T, R> Iterator<R> transform(Iterator<T> originalIterator, Function<T, R> apply) {
        return new StreamIterator() {
            @Override
            public R next() {
                return apply.apply(originalIterator.next());
            }
            @Override
            public boolean hasNext() {
                return originalIterator.hasNext();
            }
        };
    }
}
