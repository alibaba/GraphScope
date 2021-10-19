/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.server.processor;

import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MemoryResultProcessor extends AbstractResultProcessor {
    private static final Logger logger = LoggerFactory.getLogger(NettyResultProcessor.class);

    private final int queryBatchSize;
    private final int iterationBatchSize;

    private List<QueryResult> batchTransformList;

    private List<Object> resultList = Lists.newArrayList();
    private String queryId;

    public MemoryResultProcessor(int queryBatchSize, int iterationBatchSize, String queryId) {
        this.batchTransformList = Lists.newArrayListWithCapacity(queryBatchSize);
        this.queryBatchSize = queryBatchSize;
        this.iterationBatchSize = iterationBatchSize;
        this.queryId = queryId;
    }

    @Override
    public void process(QueryResult queryResult) {
        batchTransformList.add(queryResult);
        if (batchTransformList.size() >= queryBatchSize) {
            try {
                gremlinResultTransform.transform(
                        batchTransformList,
                        schema,
                        labelIndexNameList,
                        null,
                        iterationBatchSize,
                        resultList,
                        queryId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            batchTransformList = Lists.newArrayListWithCapacity(queryBatchSize);
        }
    }

    @Override
    public void finish() {
        if (!batchTransformList.isEmpty()) {
            try {
                gremlinResultTransform.transform(
                        batchTransformList,
                        schema,
                        labelIndexNameList,
                        null,
                        iterationBatchSize,
                        resultList,
                        queryId);
            } catch (Exception e) {
                return;
            }
        }
        gremlinResultTransform.finish();
    }

    @Override
    public long total() {
        return resultList.size();
    }

    public List<Object> getResultList() {
        return resultList;
    }
}
