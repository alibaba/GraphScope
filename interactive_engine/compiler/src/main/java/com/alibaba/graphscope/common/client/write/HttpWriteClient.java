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

package com.alibaba.graphscope.common.client.write;

import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.exception.ExecutionException;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.runtime.write.BatchEdgeRequest;
import com.alibaba.graphscope.common.ir.runtime.write.Request;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;
import com.alibaba.graphscope.interactive.client.Session;
import com.alibaba.graphscope.interactive.client.common.Result;
import com.alibaba.graphscope.interactive.client.common.Status;
import com.alibaba.graphscope.interactive.models.VertexEdgeRequest;
import com.google.common.base.Preconditions;

import java.util.List;

public class HttpWriteClient {
    private final Session session;

    public HttpWriteClient(Session session) {
        this.session = session;
    }

    public void submit(
            ExecutionRequest request,
            ExecutionResponseListener listener,
            IrMeta irMeta,
            QueryTimeoutConfig timeoutConfig,
            QueryLogger queryLogger) {
        LogicalPlan plan = request.getRequestLogical();
        Preconditions.checkArgument(
                plan.getMode() == LogicalPlan.Mode.WRITE_ONLY,
                "plan mode " + plan.getMode() + " is unsupported in http write client");
        String graphId = (String) irMeta.getGraphId().getId();
        List<Request> requests = (List<Request>) request.getRequestPhysical().getContent();
        for (Request req : requests) {
            switch (req.type) {
                case ADD_VERTEX:
                    Result res = session.addVertex(graphId, (VertexEdgeRequest) req.request);
                    if (!res.isOk()) {
                        listener.onError(
                                new ExecutionException(
                                        "write error from execution. " + toString(res.getStatus()),
                                        null));
                        return;
                    }
                    break;
                case ADD_EDGE:
                    Result result = session.addEdge(graphId, (BatchEdgeRequest) req.request);
                    if (!result.isOk()) {
                        listener.onError(
                                new ExecutionException(
                                        "write error from execution. "
                                                + toString(result.getStatus()),
                                        null));
                        return;
                    }
                    break;
            }
        }
        listener.onCompleted();
    }

    private String toString(Status status) {
        return status.getCode() + ":" + status.getMessage();
    }

    public void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }
}
