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

import com.alibaba.maxgraph.result.BulkResult;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.alibaba.maxgraph.cache.CacheFactory;
import com.alibaba.maxgraph.server.AbstractMixedOpProcessor;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NettyResultProcessor extends AbstractResultProcessor {
    private static final Logger logger = LoggerFactory.getLogger(NettyResultProcessor.class);

    private final String queryId;
    private final String queryScript;
    private final Context context;
    private final int queryBatchSize;
    private final int iterationBatchSize;

    private List<Object> resultList;
    private long totalCount;
    private List<QueryResult> batchTransformList;
    private boolean queryCacheFlag;
    private Cache<String, List<Object>> queryCache;
    private Throwable throwable = null;

    public NettyResultProcessor(
            String queryId,
            String queryScript,
            Context context,
            int queryBatchSize,
            int iterationBatchSize,
            boolean queryCacheFlag) {
        this.queryId = queryId;
        this.queryScript = queryScript;
        this.context = context;
        this.batchTransformList = Lists.newArrayListWithCapacity(queryBatchSize);
        this.queryBatchSize = queryBatchSize;
        this.iterationBatchSize = iterationBatchSize;

        this.queryCacheFlag = queryCacheFlag;
        if (this.queryCacheFlag) {
            queryCache = CacheFactory.getCacheFactory().getQueryCache();
            resultList = Lists.newArrayList();
        } else {
            resultList = null;
        }
    }

    @Override
    public synchronized void process(QueryResult queryResult) {
        if (queryResult instanceof BulkResult) {
            List<QueryResult> resultList = ((BulkResult) queryResult).getResultList();
            batchTransformList.addAll(((BulkResult) queryResult).getResultList());
            totalCount += resultList.size();
        } else {
            batchTransformList.add(queryResult);
            totalCount++;
        }
        if (batchTransformList.size() >= queryBatchSize) {
            try {
                logger.info(
                        "start transform "
                                + batchTransformList.size()
                                + " results for query "
                                + queryId);
                gremlinResultTransform.transform(
                        batchTransformList,
                        schema,
                        labelIndexNameList,
                        context,
                        iterationBatchSize,
                        resultList,
                        queryId);
                logger.info(
                        "end transform "
                                + batchTransformList.size()
                                + " results for query "
                                + queryId);
            } catch (Exception e) {
                throwable = e;
            }

            batchTransformList = Lists.newArrayListWithCapacity(queryBatchSize);
        }
    }

    @Override
    public void finish() {
        if (null != throwable) {
            AbstractMixedOpProcessor.writeResultList(
                    context, Lists.newArrayList(throwable.toString()), ResponseStatusCode.SUCCESS);
            return;
        }

        if (!batchTransformList.isEmpty()) {
            try {
                logger.info(
                        "start finish transform "
                                + batchTransformList.size()
                                + " results for query "
                                + queryId);
                gremlinResultTransform.transform(
                        batchTransformList,
                        schema,
                        labelIndexNameList,
                        context,
                        iterationBatchSize,
                        resultList,
                        queryId);
                logger.info(
                        "end finish transform "
                                + batchTransformList.size()
                                + " results for query "
                                + queryId);
            } catch (Exception e) {
                logger.error("Query fail for transform result", e);
                AbstractMixedOpProcessor.writeResultList(
                        context, Lists.newArrayList(e.toString()), ResponseStatusCode.SERVER_ERROR);
                return;
            }
        }
        gremlinResultTransform.finish();
        if (queryCacheFlag) {
            queryCache.put(queryScript, resultList);
        }
    }

    @Override
    public long total() {
        return totalCount;
    }
}
