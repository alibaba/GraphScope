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
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.server.Context;

import java.util.Map;

public class NettyTraverserVertexProcessor extends NettyVertexRpcProcessor {
    public NettyTraverserVertexProcessor(Context context, int batchSize, boolean queryCacheFlag) {
        super(context, batchSize, queryCacheFlag);
    }

    @Override
    public void process(Object obj) {
        if (obj instanceof Map) {
            //Temporary process for gremlin result
            Map.class.cast(obj).remove("id");
        }
        DefaultRemoteTraverser value = new DefaultRemoteTraverser<>(obj, 1);
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
}
