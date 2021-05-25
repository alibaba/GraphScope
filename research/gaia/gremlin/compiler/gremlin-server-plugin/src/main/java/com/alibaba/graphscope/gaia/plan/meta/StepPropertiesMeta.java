package com.alibaba.graphscope.gaia.plan.meta;

import com.alibaba.graphscope.gaia.plan.meta.object.StepId;
import org.apache.tinkerpop.gremlin.process.traversal.Step;

import java.util.List;

public class StepPropertiesMeta {
    private List<String> properties;
    private StepId stepId;
    private Step step;

    public StepPropertiesMeta(List<String> properties, StepId stepId, Step step) {
        this.properties = properties;
        this.stepId = stepId;
        this.step = step;
    }

    public List<String> getProperties() {
        return properties;
    }

    public StepId getStepId() {
        return stepId;
    }

    public Step getStep() {
        return step;
    }

    public void addProperties(List<String> properties) {
        this.properties.addAll(properties);
    }

    public void setStepId(StepId stepId) {
        this.stepId = stepId;
    }
}
