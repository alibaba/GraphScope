package com.alibaba.graphscope.gaia.plan.strategy.global.property.cache;

import com.alibaba.graphscope.gaia.plan.meta.PropertiesMapMeta;
import com.alibaba.graphscope.gaia.plan.meta.StepPropertiesMeta;
import com.alibaba.graphscope.gaia.plan.meta.object.Edge;
import com.alibaba.graphscope.gaia.plan.meta.object.GraphElement;
import com.alibaba.graphscope.gaia.plan.strategy.PropertyIdentityStep;
import com.alibaba.graphscope.gaia.plan.meta.TraversalsMeta;
import com.alibaba.graphscope.gaia.plan.meta.object.StepId;
import com.alibaba.graphscope.gaia.plan.meta.object.TraversalId;
import com.alibaba.graphscope.gaia.plan.strategy.global.GlobalTraversalStrategy;
import com.alibaba.graphscope.gaia.plan.translator.TraversalMetaCollector;
import com.alibaba.graphscope.gaia.plan.translator.builder.MetaConfig;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalMetaBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.*;

public class PreCachePropertyStrategy implements GlobalTraversalStrategy {
    private static final PreCachePropertyStrategy INSTANCE = new PreCachePropertyStrategy();

    private PreCachePropertyStrategy() {
    }

    public static PreCachePropertyStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalMetaBuilder root = createRootTraversalBuilder(traversal);
        new TraversalMetaCollector(root).translate();
        PropertiesMapMeta propertiesMeta = (PropertiesMapMeta) root.getConfig(MetaConfig.ELEMENT_PROPERTIES);
        transformPropertiesMeta(propertiesMeta);
        List<StepPropertiesMeta> stepPropertiesMetas = new ArrayList<>();
        for (GraphElement element : propertiesMeta.getAllObjects()) {
            stepPropertiesMetas.add(propertiesMeta.get(element).get());
        }
        addPreCachePropertiesStep(TraversalId.root(), traversal, stepPropertiesMetas);
    }

    protected TraversalMetaBuilder createRootTraversalBuilder(Traversal.Admin traversal) {
        return new TraversalMetaBuilder(traversal, null, TraversalId.root())
                .addConfig(MetaConfig.TRAVERSALS_PATH, new TraversalsMeta())
                .addConfig(MetaConfig.ELEMENT_PROPERTIES, new PropertiesMapMeta());
    }

    // for E: collect properties into PropertyCacheStep
    // for V: add after as step instead of before select step
    protected void transformPropertiesMeta(PropertiesMapMeta propertiesMapMeta) {
        for (GraphElement element : propertiesMapMeta.getAllObjects()) {
            StepPropertiesMeta meta = propertiesMapMeta.get(element).get();
            StepId firstUsed = meta.getStepId();
            StepId asStep = propertiesMapMeta.getAsStep(element);
            StepId orderStep = propertiesMapMeta.getOrderStepId(element);
            if (element instanceof Edge) {
                ((Edge) element).getOutputStep().addPropertiesToCache(meta.getProperties());
                propertiesMapMeta.delete(element);
            } else {
                // pre cache property before order step to guarantee order
                if (orderStep != null && firstUsed.getTraversalId().equals(orderStep.getTraversalId()) && firstUsed.getStepId() > orderStep.getStepId()
                        && (asStep == null || orderStep.getTraversalId().equals(asStep.getTraversalId()) && asStep.getStepId() >= orderStep.getStepId())) {
                    meta.setStepId(new StepId(orderStep.getTraversalId(), orderStep.getStepId()));
                } else if (asStep == null || firstUsed.getTraversalId().equals(asStep.getTraversalId()) && firstUsed.getStepId() <= asStep.getStepId()) {
                    // do nothing
                } else {
                    meta.setStepId(new StepId(asStep.getTraversalId(), asStep.getStepId() + 1));
                }
            }
        }
    }

    protected void addPreCachePropertiesStep(TraversalId traversalId, Traversal.Admin traversal, List<StepPropertiesMeta> stepPropertiesMetas) {
        // skip other traversal
        if (!(traversal instanceof DefaultTraversal)) {
            return;
        }
        Step t = traversal.getEndStep();
        while (!(t instanceof EmptyStep)) {
            StepId stepId = new StepId(traversalId, TraversalHelper.stepIndex(t, traversal));
            if (t instanceof TraversalParent) {
                if (t instanceof RepeatStep || t instanceof UnionStep) {
                    int i = 0;
                    for (Traversal.Admin k : ((TraversalParent) t).getGlobalChildren()) {
                        addPreCachePropertiesStep(traversalId.fork(stepId.getStepId(), i), k, stepPropertiesMetas);
                        ++i;
                    }
                } else {
                    int i = 0;
                    for (Traversal.Admin k : ((TraversalParent) t).getLocalChildren()) {
                        addPreCachePropertiesStep(traversalId.fork(stepId.getStepId(), i), k, stepPropertiesMetas);
                        ++i;
                    }
                }
            }
            t = t.getPreviousStep();
        }
        t = traversal.getEndStep();
        setLocked((DefaultTraversal) traversal, false);
        while (!(t instanceof EmptyStep)) {
            StepId stepId = new StepId(traversalId, TraversalHelper.stepIndex(t, traversal));
            if (!(t instanceof PropertyIdentityStep)) {
                StepPropertiesMeta stepPropertiesMeta = findStepPropertiesMeta(stepPropertiesMetas, stepId);
                if (stepPropertiesMeta != null) {
                    ToFetchProperties toFetch = stepPropertiesMeta.getProperties();
                    // dedup
                    toFetch.dedupProperties();
                    // add IdentityStep before this step
                    traversal.addStep(stepId.getStepId(), new PropertyIdentityStep(traversal, toFetch));
                }
            }
            t = t.getPreviousStep();
        }
        setLocked((DefaultTraversal) traversal, true);
    }

    protected StepPropertiesMeta findStepPropertiesMeta(List<StepPropertiesMeta> stepPropertiesMetas, StepId stepId) {
        for (StepPropertiesMeta meta : stepPropertiesMetas) {
            if (meta.getStepId().equals(stepId)) {
                return meta;
            }
        }
        return null;
    }
}
