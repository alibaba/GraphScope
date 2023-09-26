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

import com.alibaba.graphscope.common.jna.type.FfiKeyType;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gremlin.plugin.step.ExpandFusionStep;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class LabelParser {
    public Object parseLabelInProjectResults(Map originalValues, Step step) {
        return parseLabel(originalValues, StringUtils.EMPTY, step, EmptyStep.instance());
    }

    /**
     * parse label id to name for each value in {@code values}
     * @param values original values before parsing
     * @param tag with stepOrTraversal help to decide the {@code LabelType}
     * @param stepOrTraversal
     * @param parent is the parent of {@code stepOrTraversal} in gremlin traversal
     * @return
     */
    public Object parseLabel(Object values, String tag, Object stepOrTraversal, Step parent) {
        if (stepOrTraversal instanceof SelectOneStep || stepOrTraversal instanceof SelectStep) {
            Iterator<Map.Entry<String, Traversal.Admin>> projectIterator =
                    TraversalParentTransformFactory.PROJECT_BY_STEP
                            .getProjectTraversals((TraversalParent) stepOrTraversal)
                            .entrySet()
                            .iterator();
            Iterator<Map.Entry> valuesIterator = ((Map) values).entrySet().iterator();
            while (valuesIterator.hasNext() && projectIterator.hasNext()) {
                Map.Entry valuesEntry = valuesIterator.next();
                Map.Entry<String, Traversal.Admin> projectEntry = projectIterator.next();
                Traversal.Admin admin = projectEntry.getValue();
                valuesEntry.setValue(
                        parseLabel(
                                valuesEntry.getValue(),
                                projectEntry.getKey(),
                                (admin == null || admin.getSteps().isEmpty())
                                        ? admin
                                        : admin.getEndStep(),
                                (Step) stepOrTraversal));
            }
            return values;
        } else if (values instanceof Map) {
            Iterator<Map.Entry> valuesIterator = ((Map) values).entrySet().iterator();
            while (valuesIterator.hasNext()) {
                Map.Entry valuesEntry = valuesIterator.next();
                if (valuesEntry.getValue() instanceof Map) {
                    parseLabel(valuesEntry.getValue(), tag, stepOrTraversal, parent);
                } else if (!(stepOrTraversal instanceof ElementMapStep)
                        || valuesEntry.getKey().equals(T.label)) {
                    valuesEntry.setValue(
                            parseLabelByType(
                                    valuesEntry.getValue(),
                                    getLabelType(tag, stepOrTraversal, parent)));
                }
            }
            return values;
        } else {
            return parseLabelByType(values, getLabelType(tag, stepOrTraversal, parent));
        }
    }

    private Object parseLabelByType(Object original, LabelType type) {
        switch (type) {
            case VERTEX_LABEL:
                return parseLabelByFfiType(original, FfiKeyType.Entity);
            case EDGE_LABEL:
                return parseLabelByFfiType(original, FfiKeyType.Relation);
            default:
                return original;
        }
    }

    private Object parseLabelByFfiType(Object label, FfiKeyType ffiKeyType) {
        if (label instanceof Number) {
            return ParserUtils.getKeyName(
                    Common.NameOrId.newBuilder().setId(((Number) label).intValue()).build(),
                    ffiKeyType);
        } else if (label instanceof List) {
            List parseLabels = Lists.newArrayList();
            for (Object o : (List) label) {
                parseLabels.add(parseLabelByFfiType(o, ffiKeyType));
            }
            return parseLabels;
        } else {
            return label;
        }
    }

    private LabelType getLabelType(String tag, Object traversalOrStep, Step parent) {
        if (!containsLabel(traversalOrStep)) {
            return LabelType.NONE;
        }
        Step tagStep = EmptyStep.instance(); // step aliased as 'tag'
        if (traversalOrStep instanceof Traversal) {
            tagStep = parent;
        } else if (traversalOrStep instanceof Step) {
            tagStep =
                    (parent instanceof SelectStep || parent instanceof SelectOneStep)
                            ? parent
                            : (Step) traversalOrStep;
        }
        tagStep = tagStep.getPreviousStep();
        while (tagStep != EmptyStep.instance()) {
            if (ObjectUtils.isEmpty(tag) || tagStep.getLabels().contains(tag)) {
                while (tagStep != EmptyStep.instance()
                        && GremlinResultAnalyzer.isSameInAndOutputType(tagStep)) {
                    tagStep = tagStep.getPreviousStep();
                }
                break;
            } else {
                tagStep = tagStep.getPreviousStep();
            }
        }
        if (tagStep instanceof GraphStep) {
            return ((GraphStep) tagStep).returnsVertex()
                    ? LabelType.VERTEX_LABEL
                    : LabelType.EDGE_LABEL;
        } else if (tagStep instanceof ExpandFusionStep) {
            switch (((ExpandFusionStep) tagStep).getExpandOpt()) {
                case Vertex:
                    return LabelType.VERTEX_LABEL;
                case Edge:
                    return LabelType.EDGE_LABEL;
                case Degree:
                default:
                    return LabelType.NONE;
            }
        } else if (tagStep instanceof VertexStep) {
            return ((VertexStep) tagStep).returnsVertex()
                    ? LabelType.VERTEX_LABEL
                    : LabelType.EDGE_LABEL;
        } else if (tagStep instanceof EdgeVertexStep || tagStep instanceof EdgeOtherVertexStep) {
            return LabelType.VERTEX_LABEL;
        } else {
            return LabelType.NONE;
        }
    }

    // check if output of the step or traversal contains label id
    private boolean containsLabel(Object traversalOrStep) {
        return (traversalOrStep instanceof TokenTraversal
                        && ((TokenTraversal) traversalOrStep)
                                .getToken()
                                .equals(T.label)) // select("a").by(T.label)
                || traversalOrStep instanceof LabelStep // g.V().label()
                || traversalOrStep instanceof ElementMapStep; // g.V().elementMap()
    }

    private enum LabelType {
        VERTEX_LABEL,
        EDGE_LABEL,
        NONE
    }
}
