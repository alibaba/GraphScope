package com.alibaba.graphscope.gremlin.result;

import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.exception.UnsupportedStepException;
import com.alibaba.graphscope.gremlin.plugin.step.PathExpandStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect.TinkerGraphStep;

import java.util.List;

public class GremlinResultAnalyzer {
    public static GremlinResultParser analyze(Traversal traversal) {
        List<Step> steps = traversal.asAdmin().getSteps();
        GremlinResultParser parserType = GremlinResultParserFactory.GRAPH_ELEMENT;
        for (Step step : steps) {
            if (Utils.equalClass(step, GraphStep.class)
                    || Utils.equalClass(step, TinkerGraphStep.class) || Utils.equalClass(step, VertexStep.class)
                    || Utils.equalClass(step, EdgeVertexStep.class) || Utils.equalClass(step, EdgeOtherVertexStep.class)
                    || Utils.equalClass(step, PathExpandStep.class)) {
                parserType = GremlinResultParserFactory.GRAPH_ELEMENT;
            } else if (Utils.equalClass(step, CountGlobalStep.class)) {
                parserType = GremlinResultParserFactory.SINGLE_VALUE;
            } else if (Utils.equalClass(step, SelectOneStep.class) || Utils.equalClass(step, SelectStep.class)
                    || Utils.equalClass(step, PropertiesStep.class) || Utils.equalClass(step, PropertyMapStep.class)) {
                parserType = GremlinResultParserFactory.PROJECT_VALUE;
            } else if (Utils.equalClass(step, GroupCountStep.class) || Utils.equalClass(step, GroupStep.class)) {
                parserType = GremlinResultParserFactory.GROUP;
            } else if (Utils.equalClass(step, UnionStep.class)) {
                parserType = GremlinResultParserFactory.UNION;
            } else if (Utils.equalClass(step, HasStep.class) || Utils.equalClass(step, DedupGlobalStep.class)
                    || Utils.equalClass(step, RangeGlobalStep.class) || Utils.equalClass(step, OrderGlobalStep.class)
                    || Utils.equalClass(step, IsStep.class) || Utils.equalClass(step, WherePredicateStep.class)
                    || Utils.equalClass(step, TraversalFilterStep.class) || Utils.equalClass(step, WhereTraversalStep.class)
                    || Utils.equalClass(step, NotStep.class)) {
                // do nothing;
            } else {
                throw new UnsupportedStepException(step.getClass(), "unimplemented yet");
            }
        }
        return parserType;
    }
}