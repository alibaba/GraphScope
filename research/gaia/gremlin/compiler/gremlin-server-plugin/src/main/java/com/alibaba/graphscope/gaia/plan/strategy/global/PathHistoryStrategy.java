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
package com.alibaba.graphscope.gaia.plan.strategy.global;

import com.alibaba.graphscope.gaia.plan.meta.*;
import com.alibaba.graphscope.gaia.plan.meta.object.Lifetime;
import com.alibaba.graphscope.gaia.plan.meta.object.StepId;
import com.alibaba.graphscope.gaia.plan.meta.object.TraversalId;
import com.alibaba.graphscope.gaia.plan.meta.object.TraverserElement;
import com.alibaba.graphscope.gaia.plan.strategy.GaiaGraphStep;
import com.alibaba.graphscope.gaia.plan.translator.TraversalMetaCollector;
import com.alibaba.graphscope.gaia.plan.translator.builder.MetaConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalMetaBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PathHistoryStrategy implements GlobalTraversalStrategy {
    private static final Logger logger = LoggerFactory.getLogger(PathHistoryStrategy.class);
    private static final PathHistoryStrategy INSTANCE = new PathHistoryStrategy();
    private static boolean isRemoveTagOn = false;
    private static boolean isLabelPathRequireOn = false;

    private PathHistoryStrategy() {
    }

    public static PathHistoryStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (isRemoveTagOn && !isLabelPathRequireOn) {
            logger.error("RemoveTag optimization can only be turned on when LabeledPathRequirement is in use.");
            return;
        }
        setLocked((DefaultTraversal) traversal, false);
        TraversalMetaBuilder root = createRootTraversalBuilder(traversal);
        new TraversalMetaCollector(root).translate();
        TraversalsMeta<TraversalId, LifetimeMeta> traversalsLife = (TraversalsMeta) root.getConfig(MetaConfig.TRAVERSALS_LIFETIME);
        TraversalsMeta<TraversalId, TraverserRequirementMeta> traversalsRequire = (TraversalsMeta) root.getConfig(MetaConfig.TRAVERSALS_REQUIREMENT);
        Meta<StepId, Set<String>> removeTag = new DefaultMapMeta();
        Set<String> unUsedTags = new HashSet<>();
        if (traversalsLife != null) {
            for (TraversalId traversalId : traversalsLife.getAllObjects()) {
                LifetimeMeta lifetime = traversalsLife.get(traversalId).get();
                // reverse
                for (TraverserElement key : lifetime.getAllObjects()) {
                    Lifetime value = lifetime.get(key).get();
                    // created in this traversal but never used by any steps
                    if (value.getLastStepId() == null && value.getStartStepId() != null) {
                        unUsedTags.add(lifetime.getLabel(key));
                    }
                    // remove tags
                    if (value.getLastStepId() != null && value.getLastStepId() != StepId.KEEP_STEP_ID) {
                        if (!removeTag.get(value.getLastStepId()).isPresent()) {
                            removeTag.add(value.getLastStepId(), new HashSet<>());
                        }
                        removeTag.get(value.getLastStepId()).get().add(lifetime.getLabel(key));
                    }
                }
            }
        }
        if (traversalsRequire != null) {
            for (StepId stepId : removeTag.getAllObjects()) {
                // delay remove tags after path
                Optional<TraverserRequirementMeta> requirementMeta = traversalsRequire.get(stepId.getTraversalId());
                if (requirementMeta.isPresent() && requirementMeta.get().getTraverserRequirement() == TraverserRequirement.PATH
                        && requirementMeta.get().getPathStepId().getTraversalId().equals(stepId.getTraversalId())
                        && requirementMeta.get().getPathStepId().getStepId() >= stepId.getStepId()) {
                    requirementMeta.get().addAllRemoveTags(removeTag.get(stepId).get());
                    removeTag.delete(stepId);
                }
            }
        }
        Step step = traversal.getStartStep();
        do {
            transformTraverser(TraversalId.root(), step, traversalsRequire, removeTag, unUsedTags);
            step = step.getNextStep();
        } while (!(step instanceof EmptyStep));

        if (traversalsRequire != null && traversal.getStartStep() instanceof GaiaGraphStep) {
            // source
            ((GaiaGraphStep) traversal.getStartStep()).setTraverserRequirement(traversalsRequire.get(TraversalId.root()).get().getTraverserRequirement());
        }
        setLocked((DefaultTraversal) traversal, true);
    }

    protected TraversalMetaBuilder createRootTraversalBuilder(Traversal.Admin traversal) {

        TraversalMetaBuilder builder = new TraversalMetaBuilder(traversal, null, TraversalId.root())
                .addConfig(MetaConfig.TRAVERSALS_PATH, new TraversalsMeta());
        if (isRemoveTagOn) {
            builder.addConfig(MetaConfig.TRAVERSALS_LIFETIME, new TraversalsMeta());
        }
        if (isLabelPathRequireOn) {
            builder.addConfig(MetaConfig.TRAVERSALS_REQUIREMENT, new TraversalsMeta());
        }
        return builder;
    }

    public void transformTraverser(TraversalId traversalId, Step t, TraversalsMeta<TraversalId, TraverserRequirementMeta> traversalsRequire,
                                   Meta<StepId, Set<String>> removeTags, Set<String> unUsedTags) {
        Traversal.Admin traversal = t.getTraversal();
        // skip
        if (traversal instanceof EmptyTraversal) return;
        if (unUsedTags != null && !unUsedTags.isEmpty()) {
            unUsedTags.forEach(k -> t.removeLabel(k));
        }
        StepId stepId = new StepId(traversalId, TraversalHelper.stepIndex(t, t.getTraversal()));
        setLocked((DefaultTraversal) traversal, false);
        if (traversalsRequire != null && traversalsRequire.get(traversalId).isPresent()
                && traversalsRequire.get(traversalId).get().getTraverserRequirement() == TraverserRequirement.PATH
                && traversalsRequire.get(traversalId).get().getPathStepId().equals(stepId)) {
            Set<String> delayRemovetags = traversalsRequire.get(traversalId).get().getRemoveTags();
            int pathStepIdx = stepId.getStepId();
            // no need to remove tags at the end of traversal
            if (t != traversal.getEndStep()) {
                Step transformStep = new TransformTraverserStep(traversal, TraverserRequirement.LABELED_PATH);
                // add TransformTraverserStep after path()
                traversal.addStep(pathStepIdx + 1, transformStep);
                updateRemoveTags(removeTags, pathStepIdx);
                if (!delayRemovetags.isEmpty()) {
                    removeTags.add(new StepId(traversalId, pathStepIdx + 1), delayRemovetags);
                }
            }
        }
        // add RemovePathHistoryStep after this step
        if (removeTags.get(stepId).isPresent()) {
            traversal.addStep(stepId.getStepId() + 1, new RemovePathHistoryStep(traversal, removeTags.get(stepId).get()));
            updateRemoveTags(removeTags, stepId.getStepId());
        }
        setLocked((DefaultTraversal) traversal, true);
        if (t instanceof TraversalParent) {
            if (t instanceof RepeatStep || t instanceof UnionStep) {
                int i = 0;
                for (Traversal.Admin k : ((TraversalParent) t).getGlobalChildren()) {
                    Step s = k.getStartStep();
                    do {
                        transformTraverser(traversalId.fork(stepId.getStepId(), i), s, traversalsRequire, removeTags, unUsedTags);
                        s = s.getNextStep();
                    } while (!(s instanceof EmptyStep));
                    ++i;
                }
            } else {
                int i = 0;
                for (Traversal.Admin k : ((TraversalParent) t).getLocalChildren()) {
                    Step s = k.getStartStep();
                    do {
                        transformTraverser(traversalId.fork(stepId.getStepId(), i), s, traversalsRequire, removeTags, unUsedTags);
                        s = s.getNextStep();
                    } while (!(s instanceof EmptyStep));
                    ++i;
                }
            }
        }
    }

    protected void updateRemoveTags(Meta<StepId, Set<String>> removeTagsMeta, int startPos) {
        Meta<StepId, Set<String>> replace = new DefaultMapMeta<>();
        for (StepId stepId : removeTagsMeta.getAllObjects()) {
            if (stepId.getStepId() > startPos) {
                replace.add(new StepId(stepId.getTraversalId(), stepId.getStepId() + 1), removeTagsMeta.get(stepId).get());
            } else {
                replace.add(stepId, removeTagsMeta.get(stepId).get());
            }
        }
        removeTagsMeta.clear();
        for (StepId stepId : replace.getAllObjects()) {
            removeTagsMeta.add(stepId, replace.get(stepId).get());
        }
    }

    public static void setIsRemoveTagOn(boolean isRemoveTagOn) {
        PathHistoryStrategy.isRemoveTagOn = isRemoveTagOn;
    }

    public static void setIsPathRequireOn(boolean isLabelPathRequireOn) {
        PathHistoryStrategy.isLabelPathRequireOn = isLabelPathRequireOn;
    }
}
