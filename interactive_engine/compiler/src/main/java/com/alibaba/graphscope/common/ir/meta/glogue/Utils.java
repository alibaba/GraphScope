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

package com.alibaba.graphscope.common.ir.meta.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.ExtendEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.*;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Utils {
    public static PatternVertex getExtendFromVertex(PatternEdge edge, PatternVertex target) {
        if (edge.getSrcVertex().equals(target)) {
            return edge.getDstVertex();
        } else if (edge.getDstVertex().equals(target)) {
            return edge.getSrcVertex();
        } else {
            throw new IllegalArgumentException(
                    "target vertex must be one of the edge's src or dst, target: "
                            + target
                            + ", edge: "
                            + edge);
        }
    }

    public static PatternDirection getExtendDirection(PatternEdge edge, PatternVertex target) {
        if (edge.isBoth()) {
            return PatternDirection.BOTH;
        } else if (edge.getSrcVertex().equals(target)) {
            return PatternDirection.IN;
        } else if (edge.getDstVertex().equals(target)) {
            return PatternDirection.OUT;
        } else {
            throw new IllegalArgumentException(
                    "target vertex must be one of the edge's src or dst, target: "
                            + target
                            + ", edge: "
                            + edge);
        }
    }

    public static List<Integer> getVertexTypeIds(RelNode rel) {
        List<RelDataTypeField> fields = rel.getRowType().getFieldList();
        Preconditions.checkArgument(
                !fields.isEmpty() && fields.get(0).getType() instanceof GraphSchemaType,
                "graph operator should have graph schema type");
        GraphSchemaType schemaType = (GraphSchemaType) fields.get(0).getType();
        GraphLabelType labelType = schemaType.getLabelType();
        return labelType.getLabelsEntry().stream()
                .map(k -> k.getLabelId())
                .collect(Collectors.toList());
    }

    public static List<EdgeTypeId> getEdgeTypeIds(RelNode rel) {
        List<RelDataTypeField> fields = rel.getRowType().getFieldList();
        Preconditions.checkArgument(
                !fields.isEmpty() && fields.get(0).getType() instanceof GraphSchemaType,
                "graph operator should have graph schema type");
        GraphSchemaType schemaType = (GraphSchemaType) fields.get(0).getType();
        GraphLabelType labelType = schemaType.getLabelType();
        return labelType.getLabelsEntry().stream()
                .map(
                        k ->
                                new EdgeTypeId(
                                        Objects.requireNonNull(k.getSrcLabelId()),
                                        Objects.requireNonNull(k.getDstLabelId()),
                                        k.getLabelId()))
                .collect(Collectors.toList());
    }

    /**
     * The pattern should satisfy the following conditions simultaneously:
     * 1. patternSize <= maxPatternSizeInGlogue
     * 2. no both directions for each edge
     * 3. no fuzzy types in each vertex or edge
     * 4. no filter conditions in each vertex or edge
     * @param pattern
     * @return
     */
    public static boolean canLookUpFromGlogue(Pattern pattern, int maxPatternSizeInGlogue) {
        if (pattern.getVertexNumber() > maxPatternSizeInGlogue) {
            return false;
        }
        for (PatternVertex vertex : pattern.getVertexSet()) {
            if (vertex.getVertexTypeIds().size() != 1) {
                return false;
            }
            ElementDetails details = vertex.getElementDetails();
            if (details != null
                    && (Double.compare(details.getSelectivity(), 1.0d) != 0
                            || details.isOptional())) {
                return false;
            }
        }
        for (PatternEdge edge : pattern.getEdgeSet()) {
            if (edge.getEdgeTypeIds().size() != 1) {
                return false;
            }
            if (edge.isBoth()) return false;
            ElementDetails details = edge.getElementDetails();
            if (details != null
                    && (Double.compare(details.getSelectivity(), 1.0d) != 0
                            || details.getRange() != null
                            || details.isOptional())) {
                return false;
            }
        }
        return true;
    }

    // convert `ExtendEdge` to `PatternEdge`
    public static PatternEdge convert(
            ExtendEdge edge, PatternVertex querySrc, PatternVertex queryDst) {
        PatternVertex src, dst;
        switch (edge.getDirection()) {
            case OUT:
            case BOTH:
                src = querySrc;
                dst = queryDst;
                break;
            case IN:
            default:
                src = queryDst;
                dst = querySrc;
                break;
        }
        return (edge.getEdgeTypeIds().size() == 1)
                ? new SinglePatternEdge(
                        src,
                        dst,
                        edge.getEdgeTypeId(),
                        0,
                        edge.getDirection() == PatternDirection.BOTH,
                        edge.getElementDetails())
                : new FuzzyPatternEdge(
                        src,
                        dst,
                        Lists.newArrayList(edge.getEdgeTypeIds()),
                        0,
                        edge.getDirection() == PatternDirection.BOTH,
                        edge.getElementDetails());
    }
}
