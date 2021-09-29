package com.alibaba.graphscope.gaia.plan.meta;

import com.alibaba.graphscope.gaia.plan.meta.object.StepId;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;

import java.util.ArrayList;
import java.util.List;

public class StepPropertiesMeta {
    private ToFetchProperties properties;
    private StepId stepId;

    public StepPropertiesMeta(ToFetchProperties properties, StepId stepId) {
        this.properties = properties;
        this.stepId = stepId;
    }

    public ToFetchProperties getProperties() {
        return properties;
    }

    public StepId getStepId() {
        return stepId;
    }

    public void addProperties(ToFetchProperties properties) {
        boolean isAll = properties.isAll() || this.properties.isAll();
        List<String> add = new ArrayList<>();
        if (!isAll) {
            add.addAll(this.properties.getProperties());
            add.addAll(properties.getProperties());
        }
        this.properties = new ToFetchProperties(isAll, add);
    }

    public void setStepId(StepId stepId) {
        this.stepId = stepId;
    }
}
