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

package com.alibaba.graphscope.gremlin.result.processor;

import com.alibaba.graphscope.gremlin.result.GremlinResultAnalyzer;
import com.alibaba.graphscope.gremlin.result.GroupResultParser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.server.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GremlinResultProcessor extends AbstractResultProcessor {
    private static Logger logger = LoggerFactory.getLogger(GremlinResultProcessor.class);

    public GremlinResultProcessor(Context writeResult, Traversal traversal) {
        super(writeResult, GremlinResultAnalyzer.analyze(traversal));
    }

    // format group result as a single map
    @Override
    protected void aggregateResults() {
        if (resultParser instanceof GroupResultParser) {
            Map groupResult = new LinkedHashMap();
            resultCollectors.forEach(
                    k -> {
                        groupResult.putAll((Map) k);
                    });
            resultCollectors.clear();
            resultCollectors.add(groupResult);
        }
    }
}
