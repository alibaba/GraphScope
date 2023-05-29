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
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.pegasus.RpcChannel;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;

import io.grpc.Status;

public class RpcExecutionClient extends ExecutionClient<RpcChannel> {
    private final Configs graphConfig;
    private final RpcClient rpcClient;

    public RpcExecutionClient(Configs graphConfig, ChannelFetcher<RpcChannel> channelFetcher) {
        super(channelFetcher);
        this.graphConfig = graphConfig;
        this.rpcClient =
                new RpcClient(
                        PegasusConfig.PEGASUS_GRPC_TIMEOUT.get(graphConfig),
                        channelFetcher.fetch());
    }

    @Override
    public void submit(ExecutionRequest request, ExecutionResponseListener listener)
            throws Exception {
        PegasusClient.JobRequest jobRequest =
                PegasusClient.JobRequest.parseFrom((byte[]) request.getRequestPhysical().build());
        PegasusClient.JobConfig jobConfig =
                PegasusClient.JobConfig.newBuilder()
                        .setJobId(request.getRequestId())
                        .setJobName(request.getRequestName())
                        .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(graphConfig))
                        .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(graphConfig))
                        .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(graphConfig))
                        .setBatchCapacity(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(graphConfig))
                        .setTimeLimit(PegasusConfig.PEGASUS_TIMEOUT.get(graphConfig))
                        .setAll(
                                com.alibaba.pegasus.service.protocol.PegasusClient.Empty
                                        .newBuilder()
                                        .build())
                        .build();
        jobRequest = jobRequest.toBuilder().setConf(jobConfig).build();
        this.rpcClient.submit(
                jobRequest,
                new ResultProcessor() {
                    @Override
                    public void process(PegasusClient.JobResponse jobResponse) {
                        try {
                            listener.onNext(
                                    IrResult.Results.parseFrom(jobResponse.getResp()).getRecord());
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void finish() {
                        listener.onCompleted();
                    }

                    @Override
                    public void error(Status status) {
                        listener.onError(status.asException());
                    }
                });
    }

    @Override
    public void close() throws Exception {
        if (rpcClient != null) {
            this.rpcClient.shutdown();
        }
    }
}
