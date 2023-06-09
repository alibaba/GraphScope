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

package com.alibaba.graphscope.gremlin.plugin.step;

import com.alibaba.graphscope.common.jna.type.PathOpt;
import com.alibaba.graphscope.common.jna.type.ResultOpt;
import com.alibaba.graphscope.gremlin.exception.ExtendGremlinStepException;
import com.google.common.base.Objects;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PathExpandStep extends ExpandFusionStep<Vertex> {
    private Traversal rangeTraversal;
    private PathOpt pathOpt;
    private ResultOpt resultOpt;
    private @Nullable String untilCondition;

    public PathExpandStep(
            final Traversal.Admin traversal,
            final Direction direction,
            final Traversal rangeTraversal,
            final String... edgeLabels) {
        super(traversal, Vertex.class, direction, edgeLabels);
        this.rangeTraversal = rangeTraversal;
        // default value
        this.pathOpt = PathOpt.Arbitrary;
        this.resultOpt = ResultOpt.EndV;
    }

    public int getLower() {
        Traversal.Admin admin = rangeTraversal.asAdmin();
        if (admin.getSteps().size() == 1 && admin.getStartStep() instanceof RangeGlobalStep) {
            RangeGlobalStep range = (RangeGlobalStep) admin.getStartStep();
            return (int) range.getLowRange();
        } else {
            throw new ExtendGremlinStepException(
                    "rangeTraversal should only have one RangeGlobalStep");
        }
    }

    public int getUpper() {
        Traversal.Admin admin = rangeTraversal.asAdmin();
        if (admin.getSteps().size() == 1 && admin.getStartStep() instanceof RangeGlobalStep) {
            RangeGlobalStep range = (RangeGlobalStep) admin.getStartStep();
            return (int) range.getHighRange();
        } else {
            throw new ExtendGremlinStepException(
                    "rangeTraversal should only have one RangeGlobalStep");
        }
    }

    public PathOpt getPathOpt() {
        return pathOpt;
    }

    public ResultOpt getResultOpt() {
        return resultOpt;
    }

    public @Nullable String getUntilCondition() {
        return untilCondition;
    }

    @Override
    public void configure(final Object... keyValues) {
        String originalKey = (String) keyValues[0];
        String originalVal = (String) keyValues[1];
        String key = toCamelCaseInsensitive(originalKey);
        String value = toCamelCaseInsensitive(originalVal);
        if (key.equals("PathOpt")) {
            if (value.equals("Arbitrary") || value.equals("Simple")) {
                this.pathOpt = PathOpt.valueOf(value);
            } else {
                throw new ExtendGremlinStepException(
                        "value "
                                + originalVal
                                + " is invalid, use ARBITRARY or SIMPLE instead (case"
                                + " insensitive)");
            }
        } else if (key.equals("ResultOpt")) {
            if (value.equals("AllV") || value.equals("EndV") || value.equals("AllVe")) {
                this.resultOpt = ResultOpt.valueOf(value);
            } else {
                throw new ExtendGremlinStepException(
                        "value "
                                + originalVal
                                + " is invalid, use ALL_V, END_V, ALL_VE instead (case"
                                + " insensitive)");
            }
        } else if (key.equals("Until")) {
            this.untilCondition = ObjectUtils.requireNonEmpty(originalVal);
        } else {
            throw new ExtendGremlinStepException(
                    "key "
                            + originalKey
                            + " is invalid, use PATH_OPT or RESULT_OPT or UNTIL instead (case"
                            + " insensitive)");
        }
    }

    // convert string of underscore format to camel case in a case-insensitive way
    private String toCamelCaseInsensitive(String underscore) {
        String[] splits = underscore.split("_");
        String camelCase = "";
        for (int i = 0; i < splits.length; ++i) {
            String split = splits[i];
            camelCase +=
                    split.isEmpty()
                            ? split
                            : Character.toUpperCase(split.charAt(0))
                                    + split.substring(1).toLowerCase();
        }
        return camelCase;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PathExpandStep that = (PathExpandStep) o;
        return Objects.equal(rangeTraversal, that.rangeTraversal)
                && pathOpt == that.pathOpt
                && resultOpt == that.resultOpt;
    }

    @Override
    public String toString() {
        return "PathExpandStep{"
                + "rangeTraversal="
                + rangeTraversal
                + ", pathOpt="
                + pathOpt
                + ", resultOpt="
                + resultOpt
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), rangeTraversal, pathOpt, resultOpt);
    }
}
