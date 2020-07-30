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
package com.alibaba.maxgraph.frontendservice.service;

import com.alibaba.maxgraph.common.server.AbstractRpcServer;
import com.alibaba.maxgraph.common.util.KryoUtils;
import com.alibaba.maxgraph.common.util.RpcUtils;
import com.alibaba.maxgraph.frontendservice.Frontend;
import com.alibaba.maxgraph.frontendservice.exceptions.PrepareException;
import com.alibaba.maxgraph.proto.*;
import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class FrontendApiServer extends AbstractRpcServer {
    private static final Logger LOG = LoggerFactory.getLogger(FrontendApiService.class);
    private Frontend frontend;

    public FrontendApiServer(Frontend frontend) throws Exception {
        this.frontend = frontend;
    }

    @Override
    public BindableService getService() {
        return new FrontendApiService();
    }

    @Override
    public String getName() {
        return "FrontendApiService";
    }

    private class FrontendApiService extends FrontendServiceGrpc.FrontendServiceImplBase {

        @Override
        public void executeQuery(QueryRequest request, StreamObserver<QueryResponse> responseStreamObserver) {
            Function<Response, GeneratedMessageV3.Builder> setResp = SessionResponse.newBuilder()::setResponse;

            try {
                RpcUtils.execute(LOG, responseStreamObserver, setResp, () -> {
                    String query = request.getQuery();
                    LOG.debug("start eval query :{}", query);
                    GraphTraversal traversal = (GraphTraversal) frontend.getGremlinExecutor().eval(query).get();
                    while (traversal.hasNext()) {
                        Object next = traversal.next();
                        QueryResponse.Builder builder = QueryResponse.newBuilder();
                        // TODO : serialize ...
                        byte[] data = KryoUtils.obj2Bytes(next, true, true);
                        builder.addResult(ByteString.copyFrom(data));
                        // TODO : What is the response ?
                        responseStreamObserver.onNext(builder.build());
                    }

                    return null;
                });
            } catch (Exception e) {
                LOG.error("create session failed:{}", e);
            }
        }

        @Override
        public void prepareQuery(com.alibaba.maxgraph.proto.PrepareRequest request,
                                 StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
            prepareAction(p -> p.prepare(request.getQuery().toByteArray(), request.getName(), false), responseObserver);
        }

        @Override
        public void removePrepare(com.alibaba.maxgraph.proto.PrepareRequest request,
                                  StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
            prepareAction(p -> p.remove(request.getName()), responseObserver);
        }

        @Override
        public void listPrepare(com.alibaba.maxgraph.proto.Empty request,
                                StreamObserver<com.alibaba.maxgraph.proto.PrepareNames> responseObserver) {
            PrepareNames.Builder builder = PrepareNames.newBuilder();
            Response.Builder resp = Response.newBuilder();
            try {
                frontend.getPreparedQueryManager().listPreparedQueryNames()
                        .forEach(builder::addName);
            } catch (Exception e) {
                resp.setErrCode(-1);
                resp.setErrMsg(e.getMessage());
            }

            builder.setResp(resp);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        private void prepareAction(final Action action, StreamObserver<com.alibaba.maxgraph.proto.Response> responseObserver) {
            Response.Builder builder = Response.newBuilder();
            try {
                action.apply(frontend.getPreparedQueryManager());
            } catch (PrepareException e) {
                builder.setErrCode(e.getErrorCode().ordinal());
                builder.setErrMsg(e.getErrorMessage().toString());
            } catch (Exception e) {
                builder.setErrCode(-1);
                builder.setErrMsg(e.getMessage());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getRealTimeMetric(MetricInfoRequest request, StreamObserver<MetricInfoResp> responseObserver) {
            MetricInfoResp.Builder build = MetricInfoResp.newBuilder();
            Function<Response, MetricInfoResp.Builder> responseBuilderFunction = build::setResp;
            RpcUtils.execute(LOG, responseObserver, responseBuilderFunction, () -> {
                MetricInfoResp result = frontend.getClientManager().getServerDataApiClient().getRealTimeMetric(request);
                build.addAllValues(result.getValuesList());
                return Iterators.singletonIterator(build.build());
            });
        }

        @Override
        public void getAllRealTimeMetrics(Request request, StreamObserver<AllMetricsInfoResp> responseObserver) {
            AllMetricsInfoResp.Builder builder = AllMetricsInfoResp.newBuilder();
            Function<Response, AllMetricsInfoResp.Builder> responseBuilderFunction = builder::setResp;
            RpcUtils.execute(LOG, responseObserver, responseBuilderFunction, () -> {
                AllMetricsInfoResp result = frontend.getClientManager().getServerDataApiClient().getAllRealTimeMetrics(request);
                builder.addAllInfo(result.getInfoList());
                return Iterators.singletonIterator(builder.build());
            });
        }
    }

    @FunctionalInterface
    private interface Action {
        void apply(PreparedQueryManager p) throws Exception;
    }
}
