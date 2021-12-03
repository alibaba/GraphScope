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
package com.alibaba.maxgraph.rpc;

import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultResultProcessor implements TimelyResultProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DefaultResultProcessor.class);
    private long totalCount = 0;
    private List<QueryResult> resultList = Lists.newArrayList();

    @Override
    public void process(QueryResult queryResult) {
        logger.info(queryResult.toString());
        resultList.add(queryResult);
        totalCount++;
    }

    @Override
    public void finish() {
    }

    @Override
    public long total() {
        return totalCount;
    }

    public List<QueryResult> getResultList() {
        return resultList;
    }
}
