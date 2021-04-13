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
package com.alibaba.graphscope.gaia.plan.strategy.shuffle;

import com.alibaba.graphscope.gaia.plan.strategy.OrderGlobalLimitStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PropertyShuffler {
    private static final Logger logger = LoggerFactory.getLogger(HasStepProperty.class);
    protected Step step;
    protected final int stepIdx;

    public PropertyShuffler(Step step) {
        this.step = step;
        this.stepIdx = TraversalHelper.stepIndex(step, step.getTraversal());
    }

    protected abstract boolean match();

    public boolean needShuffle() {
        Step previousOut = getPreviousShuffleStep(true);
        if (!match() || previousOut == null) return false;
        Step p = this.step;
        // out().as("a")
        if (p == previousOut) return true;
        p = p.getPreviousStep();
        while (!(p instanceof EmptyStep) && p != previousOut) {
            if (ShuffleStrategy.needShuffle(p)) return false;
            if (p.getTraversal().getStartStep() == p) {
                p = p.getTraversal().getParent().asStep();
            } else {
                p = p.getPreviousStep();
            }
        }
        return true;
    }

    /**
     * transform traversal for shuffle convenience, including current step
     *
     * @return next step index
     */
    public abstract int transform();

    /**
     * @param inclusive
     * @return
     */
    public Step getPreviousShuffleStep(boolean inclusive) {
        Step startStep = step.getTraversal().getStartStep();
        Step p = step;
        if (!inclusive) {
            p = p.getPreviousStep();
        }
        int nestedLevel = 1;
        while (true) {
            if (p instanceof VertexStep && ((VertexStep) p).returnsVertex()
                    || p instanceof EdgeOtherVertexStep
                    || p instanceof EdgeVertexStep && !isLocalVertex((EdgeVertexStep) p)
                    || isGlobalStep(p)) return p;
            if (p == startStep) {
                // root or come to the start of upper level
                TraversalParent parent = p.getTraversal().getParent();
                if (parent instanceof EmptyStep || nestedLevel == 0) break;
                --nestedLevel;
                p = parent.asStep();
                startStep = p.getTraversal().getStartStep();
            } else {
                p = p.getPreviousStep();
            }
        }
        return null;
    }

    public static boolean isGlobalStep(Step step) {
        return step instanceof RangeGlobalStep || step instanceof OrderGlobalStep || step instanceof OrderGlobalLimitStep
                || step instanceof GroupCountStep || step instanceof GroupStep || step instanceof CountGlobalStep
                || step instanceof DedupGlobalStep;
    }

    // outE().limit(1).outV()
    // inE().limit(1).inV()
    protected boolean isLocalVertex(EdgeVertexStep edgeVertexStep) {
        if (edgeVertexStep.getDirection() == Direction.BOTH) {
            return false;
        }
        Step startStep = edgeVertexStep.getTraversal().getStartStep();
        Step p = edgeVertexStep;
        if (p == startStep) {
            logger.error("invalid query {}", p.getTraversal());
            return false;
        }
        do {
            p = p.getPreviousStep();
            if (p instanceof VertexStep && ((VertexStep) p).returnsEdge()) {
                return ((VertexStep) p).getDirection() == edgeVertexStep.getDirection();
            }
        } while (p != startStep);
        return false;
    }
}
