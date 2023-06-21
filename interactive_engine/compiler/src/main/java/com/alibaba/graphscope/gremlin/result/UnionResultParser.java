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

package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.gaia.proto.IrResult;
import com.google.common.base.Preconditions;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;

import java.util.Iterator;

public class UnionResultParser implements GremlinResultParser {
    private final Step step;

    private UnionResultParser(Step step) {
        this.step = step;
    }

    public static UnionResultParser create(Step step) {
        return new UnionResultParser(step);
    }

    @Override
    public Object parseFrom(IrResult.Results results) {
        GremlinResultParser resultParser = getResultParser();
        return resultParser.parseFrom(results);
    }

    private GremlinResultParser getResultParser() {
        UnionStep unionStep = (UnionStep) step;
        Iterator<Traversal.Admin> subIterator = unionStep.getGlobalChildren().iterator();
        Preconditions.checkArgument(
                subIterator.hasNext(), "union step must have at least one branch traversal");
        GremlinResultParser resultParser = GremlinResultAnalyzer.analyze(subIterator.next());
        while (subIterator.hasNext()) {
            GremlinResultParser subParser = GremlinResultAnalyzer.analyze(subIterator.next());
            Preconditions.checkArgument(
                    resultParser.getClass().equals(subParser.getClass()),
                    "union step must have same branch traversal result type");
        }
        return resultParser;
    }
}
