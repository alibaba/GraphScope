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
import com.alibaba.graphscope.common.client.metric.RpcExecutorMetric;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.metric.MetricsTool;
import com.alibaba.graphscope.gaia.proto.IrResult;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.pegasus.RpcChannel;
import com.alibaba.pegasus.RpcClient;
import com.alibaba.pegasus.intf.ResultProcessor;
import com.alibaba.pegasus.service.protocol.PegasusClient;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import io.grpc.ClientInterceptors;
import io.grpc.Status;

import java.util.List;
import java.util.stream.Collectors;

/**
 * rpc client to send request to pegasus engine service
 */
public class RpcExecutionClient extends ExecutionClient<RpcChannel> {
    private final Configs graphConfig;

    public RpcExecutionClient(
            Configs graphConfig,
            ChannelFetcher<RpcChannel> channelFetcher,
            MetricsTool metricsTool) {
        super(channelFetcher);
        this.graphConfig = graphConfig;
        metricsTool.registerMetric(new RpcExecutorMetric(channelFetcher));
    }

    @Override
    public void submit(
            ExecutionRequest request,
            ExecutionResponseListener listener,
            QueryTimeoutConfig timeoutConfig,
            QueryLogger queryLogger)
            throws Exception {
        List<RpcChannel> interceptChannels =
                channelFetcher.fetch().stream()
                        .map(
                                k ->
                                        new RpcChannel(
                                                ClientInterceptors.intercept(
                                                        k.getChannel(), new RpcInterceptor())))
                        .collect(Collectors.toList());
        RpcClient rpcClient =
                new RpcClient(
                        interceptChannels,
                        ImmutableMap.of(RpcInterceptor.QUERY_LOGGER_OPTION, queryLogger));
        PegasusClient.JobRequest jobRequest =
                PegasusClient.JobRequest.newBuilder()
                        .setPlan(
                                ByteString.copyFrom(
                                        (byte[]) request.getRequestPhysical().getContent()))
                        .build();
        PegasusClient.JobConfig jobConfig =
                PegasusClient.JobConfig.newBuilder()
                        .setJobId(request.getRequestId().longValue())
                        .setJobName(request.getRequestName())
                        .setWorkers(PegasusConfig.PEGASUS_WORKER_NUM.get(graphConfig))
                        .setBatchSize(PegasusConfig.PEGASUS_BATCH_SIZE.get(graphConfig))
                        .setMemoryLimit(PegasusConfig.PEGASUS_MEMORY_LIMIT.get(graphConfig))
                        .setBatchCapacity(PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(graphConfig))
                        .setTimeLimit(timeoutConfig.getEngineTimeoutMS())
                        .setAll(
                                com.alibaba.pegasus.service.protocol.PegasusClient.Empty
                                        .newBuilder()
                                        .build())
                        .build();
        jobRequest = jobRequest.toBuilder().setConf(jobConfig).build();
        rpcClient.submit(
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
                        queryLogger.info(
                                "[query][response]: received all responses from all servers");
                    }

                    @Override
                    public void error(Status status) {
                        listener.onError(status.asException());
                        queryLogger.error("[compile]: fail to receive results from engine");
                    }
                },
                timeoutConfig.getChannelTimeoutMS());
    }

    @Override
    public void close() throws Exception {
        channelFetcher
                .fetch()
                .forEach(
                        k -> {
                            try {
                                if (k != null) {
                                    k.shutdown();
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }
}
