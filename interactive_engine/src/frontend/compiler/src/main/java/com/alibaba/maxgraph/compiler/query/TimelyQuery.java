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
package com.alibaba.maxgraph.compiler.query;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.optimizer.QueryFlowManager;
import com.alibaba.maxgraph.rpc.DefaultResultProcessor;
import com.alibaba.maxgraph.rpc.TimelyResultProcessor;
import org.apache.tinkerpop.gremlin.structure.Graph;

import static com.google.common.base.Preconditions.checkNotNull;

public class TimelyQuery {
    private final QueryFlowOuterClass.QueryInput queryInput;
    private final QueryFlowManager queryFlowManager;
    private final TimelyResultProcessor resultProcessor;
    private final Graph graph;

    public TimelyQuery(QueryFlowOuterClass.QueryInput queryInput, TimelyResultProcessor resultProcessor, Graph graph) {
        this(queryInput, null, resultProcessor, graph);
    }

    public TimelyQuery(QueryFlowManager queryFlowManager, Graph graph) {
        this(null, queryFlowManager, new DefaultResultProcessor(), graph);
    }

    public TimelyQuery(QueryFlowManager queryFlowManager, TimelyResultProcessor resultProcessor, Graph graph) {
        this(null, queryFlowManager, resultProcessor, graph);
    }

    private TimelyQuery(QueryFlowOuterClass.QueryInput queryInput, QueryFlowManager queryFlowManager, TimelyResultProcessor resultProcessor, Graph graph) {
        this.queryInput = queryInput;
        this.queryFlowManager = queryFlowManager;
        this.resultProcessor = resultProcessor;
        this.graph = graph;
    }

    public TimelyQuery(QueryFlowOuterClass.QueryInput queryInput, Graph graph) {
        this(queryInput, new DefaultResultProcessor(), graph);
    }

    public TimelyResultProcessor getResultProcessor() {
        return checkNotNull(resultProcessor);
    }

    public QueryFlowManager getQueryFlowManager() {
        return checkNotNull(queryFlowManager);
    }

    public QueryFlowOuterClass.QueryInput getQueryInput() {
        return checkNotNull(queryInput);
    }

    public Graph getGraph() {
        return graph;
    }
}
