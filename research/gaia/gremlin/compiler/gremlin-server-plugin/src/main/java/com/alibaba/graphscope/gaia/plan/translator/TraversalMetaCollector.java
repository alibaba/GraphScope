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
package com.alibaba.graphscope.gaia.plan.translator;

import com.alibaba.graphscope.gaia.plan.meta.*;
import com.alibaba.graphscope.gaia.plan.meta.object.*;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import com.alibaba.graphscope.gaia.plan.translator.builder.MetaConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.StepMetaBuilder;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalMetaBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.TokenTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Column;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class TraversalMetaCollector extends AttributeTranslator<TraversalMetaBuilder, TraverserElement> {
    public TraversalMetaCollector(TraversalMetaBuilder input) {
        super(input);
    }

    @Override
    protected Function<TraversalMetaBuilder, TraverserElement> getApplyFunc() {
        return (TraversalMetaBuilder t) -> {
            // todo: better way to locate the first occurrence of root traversal
            if (t.getMetaId().equals(TraversalId.root()) && t.getHead() == null) {
                createRootTraversalMeta(t);
            } else {
                forkTraversalMetaIfNeed(t);
            }
            Traversal.Admin admin = t.getAdmin();
            TraverserElement head = t.getHead();
            // pre-cache properties
            Meta<GraphElement, StepPropertiesMeta> elementProperties = (Meta) t.getConfig(MetaConfig.ELEMENT_PROPERTIES);
            if (elementProperties != null && admin instanceof ElementValueTraversal && head.getObject().isGraphElement()) {
                ElementValueTraversal admin1 = (ElementValueTraversal) admin;
                Step parent = StepMetaCollector.getParentOfTraversalId(t.getMetaId(), admin1);
                int stepIdx = TraversalHelper.stepIndex(parent, parent.getTraversal());
                elementProperties.add(head.getObject().getElement(),
                        new StepPropertiesMeta(new ToFetchProperties(false, Collections.singletonList(admin1.getPropertyKey())),
                                new StepId(t.getMetaId(), stepIdx)));
            }
            if (admin instanceof IdentityTraversal) {
                return head;
            }
            if (admin instanceof ColumnTraversal) {
                if (head.getObject().getClassName() != Map.Entry.class) {
                    throw new UnsupportedOperationException("ColumnTraversal has invalid input type " + head.getObject().getClassName());
                }
                if (((ColumnTraversal) admin).getColumn() == Column.keys) {
                    return new TraverserElement(head.getObject().getSub(0));
                } else {
                    return new TraverserElement(head.getObject().getSub(1));
                }
            }
            if (admin instanceof ElementValueTraversal || admin instanceof TokenTraversal) {
                return new TraverserElement(new CompositeObject(String.class));
            }
            List<Step> steps = t.getAdmin().getSteps();
            for (Step step : steps) {
                head = new StepMetaCollector(new StepMetaBuilder(step, t.getMetaId(), head).setConf(t.getConf())).translate();
            }
            return head;
        };
    }

    public static void forkTraversalMetaIfNeed(TraversalMetaBuilder t) {
        TraversalId metaId = t.getMetaId();
        TraversalsMeta<TraversalId, PathHistoryMeta> traversalsPath = (TraversalsMeta) t.getConfig(MetaConfig.TRAVERSALS_PATH);
        if (traversalsPath.get(metaId).isPresent()) {
            return;
        }
        // fork path
        PathHistoryMeta parentPath = traversalsPath.get(metaId.getParent()).get();
        PathHistoryMeta newPath = parentPath.fork();
        traversalsPath.add(metaId, newPath);
        // fork lifetime if need
        TraversalsMeta<TraversalId, LifetimeMeta> traversalsLife = (TraversalsMeta) t.getConfig(MetaConfig.TRAVERSALS_LIFETIME);
        if (traversalsLife != null) {
            LifetimeMeta newLife = new LifetimeMeta();
            newPath.getAllObjects().forEach(k -> {
                TraverserElement ele = newPath.get(k).get();
                if (!newLife.get(ele).isPresent()) {
                    newLife.add(ele, new Lifetime(null, null));
                    newLife.attachLabel(ele, k);
                }
            });
            traversalsLife.add(metaId, newLife);
        }
        // fork path requirement if need
        TraversalsMeta<TraversalId, TraverserRequirementMeta> traversalsRequire = (TraversalsMeta) t.getConfig(MetaConfig.TRAVERSALS_REQUIREMENT);
        if (traversalsRequire != null) {
            traversalsRequire.add(metaId, new TraverserRequirementMeta());
        }
    }

    public static void createRootTraversalMeta(TraversalMetaBuilder t) {
        TraversalId rootId = t.getMetaId();
        ((Meta) t.getConfig(MetaConfig.TRAVERSALS_PATH)).add(rootId, new PathHistoryMeta());
        if (t.getConfig(MetaConfig.TRAVERSALS_LIFETIME) != null) {
            ((Meta) t.getConfig(MetaConfig.TRAVERSALS_LIFETIME)).add(rootId, new LifetimeMeta());
        }
        if (t.getConfig(MetaConfig.TRAVERSALS_REQUIREMENT) != null) {
            ((Meta) t.getConfig(MetaConfig.TRAVERSALS_REQUIREMENT)).add(rootId, new TraverserRequirementMeta());
        }
    }
}
