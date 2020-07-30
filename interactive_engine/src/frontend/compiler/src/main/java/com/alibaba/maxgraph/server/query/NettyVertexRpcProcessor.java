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
package com.alibaba.maxgraph.server.query;

import com.alibaba.maxgraph.server.AbstractMixedOpProcessor;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;

import java.util.List;

public class NettyVertexRpcProcessor implements RemoteRpcProcessor {
    protected final Context context;
    protected final int batchSize;

    protected boolean queryCacheFlag;
    protected List<Object> resultList;
    protected List<Object> batchResultList;

    public NettyVertexRpcProcessor(Context context, int batchSize, boolean queryCacheFlag) {
        this.context = context;
        this.batchSize = batchSize;
        this.batchResultList = Lists.newArrayListWithCapacity(this.batchSize);
        this.queryCacheFlag = queryCacheFlag;
        if (this.queryCacheFlag) {
            resultList = Lists.newArrayList();
        }
    }

    @Override
    public void process(Object value) {
        batchResultList.add(value);
        if (batchResultList.size() >= batchSize) {
            AbstractMixedOpProcessor.writeResultList(
                    context,
                    batchResultList,
                    ResponseStatusCode.PARTIAL_CONTENT);
            batchResultList = Lists.newArrayListWithCapacity(this.batchSize);
        }
        if (queryCacheFlag) {
            resultList.add(value);
        }
    }

    @Override
    public void finish(ResponseStatusCode statusCode) {
        AbstractMixedOpProcessor.writeResultList(
                context,
                batchResultList,
                statusCode);
    }

    @Override
    public void finish() {
        if (!batchResultList.isEmpty()) {
            AbstractMixedOpProcessor.writeResultList(
                    context,
                    batchResultList,
                    ResponseStatusCode.PARTIAL_CONTENT);
        }
    }

    @Override
    public List<Object> getResultList() {
        if (queryCacheFlag) {
            return resultList;
        } else {
            throw new RuntimeException("No cache result list");
        }
    }
}
