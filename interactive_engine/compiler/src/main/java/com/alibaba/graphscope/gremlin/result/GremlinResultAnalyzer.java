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

import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.exception.UnsupportedStepException;
import com.alibaba.graphscope.gremlin.plugin.step.*;
import com.alibaba.graphscope.gremlin.plugin.step.GroupCountStep;
import com.alibaba.graphscope.gremlin.plugin.step.GroupStep;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;

import java.util.List;

public class GremlinResultAnalyzer {
    public static GremlinResultParser analyze(Traversal traversal) {
        List<Step> steps = traversal.asAdmin().getSteps();
        GremlinResultParser parserType = GremlinResultParserFactory.GRAPH_ELEMENT;
        for (Step step : steps) {
            if (Utils.equalClass(step, ScanFusionStep.class)
                    || Utils.equalClass(step, ExpandFusionStep.class)
                    || Utils.equalClass(step, EdgeVertexStep.class)
                    || Utils.equalClass(step, EdgeOtherVertexStep.class)
                    || Utils.equalClass(step, PathExpandStep.class)) {
                parserType = GremlinResultParserFactory.GRAPH_ELEMENT;
            } else if (Utils.equalClass(step, CountGlobalStep.class)
                    || Utils.equalClass(step, SumGlobalStep.class)
                    || Utils.equalClass(step, MinGlobalStep.class)
                    || Utils.equalClass(step, MaxGlobalStep.class)
                    || Utils.equalClass(step, MeanGlobalStep.class)
                    || Utils.equalClass(step, FoldStep.class)) {
                parserType = GremlinResultParserFactory.SINGLE_VALUE;
            } else if (Utils.equalClass(step, SelectOneStep.class)
                    || Utils.equalClass(step, SelectStep.class)
                    || Utils.equalClass(step, PropertiesStep.class)
                    || Utils.equalClass(step, PropertyMapStep.class)
                    || Utils.equalClass(step, TraversalMapStep.class)
                    || Utils.equalClass(step, MatchStep.class)
                    || Utils.equalClass(step, ExprStep.class)
                    || Utils.equalClass(step, IdStep.class)
                    || Utils.equalClass(step, LabelStep.class)
                    || Utils.equalClass(step, ConstantStep.class)) {
                parserType = GremlinResultParserFactory.PROJECT_VALUE;
            } else if (Utils.equalClass(step, GroupCountStep.class)
                    || Utils.equalClass(step, GroupStep.class)) {
                parserType = GroupResultParser.create(step);
            } else if (Utils.equalClass(step, UnionStep.class)) {
                parserType = GremlinResultParserFactory.UNION;
            } else if (Utils.equalClass(step, SubgraphStep.class)) {
                parserType = GremlinResultParserFactory.SUBGRAPH;
            } else if (Utils.equalClass(step, HasStep.class)
                    || Utils.equalClass(step, DedupGlobalStep.class)
                    || Utils.equalClass(step, RangeGlobalStep.class)
                    || Utils.equalClass(step, OrderGlobalStep.class)
                    || Utils.equalClass(step, IsStep.class)
                    || Utils.equalClass(step, WherePredicateStep.class)
                    || Utils.equalClass(step, TraversalFilterStep.class)
                    || Utils.equalClass(step, WhereTraversalStep.class)
                    || Utils.equalClass(step, NotStep.class)) {
                // do nothing;
            } else {
                throw new UnsupportedStepException(step.getClass(), "unimplemented yet");
            }
        }
        return parserType;
    }
}
