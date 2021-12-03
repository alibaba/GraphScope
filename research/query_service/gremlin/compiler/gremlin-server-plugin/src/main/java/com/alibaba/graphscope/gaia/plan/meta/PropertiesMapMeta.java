package com.alibaba.graphscope.gaia.plan.meta;

import com.alibaba.graphscope.gaia.plan.meta.object.GraphElement;
import com.alibaba.graphscope.gaia.plan.meta.object.StepId;

import java.util.HashMap;
import java.util.Map;

public class PropertiesMapMeta extends DefaultMapMeta<GraphElement, StepPropertiesMeta> {
    private Map<GraphElement, StepId> firstAsStepMap = new HashMap<>();
    private Map<GraphElement, StepId> orderStepId = new HashMap<>();

    @Override
    public void add(GraphElement object, StepPropertiesMeta data) {
        if (this.mapMeta.get(object) == null) {
            this.mapMeta.put(object, new StepPropertiesMeta(data.getProperties(), data.getStepId()));
        } else {
            this.mapMeta.get(object).addProperties(data.getProperties());
        }
    }

    public void addAsStep(GraphElement element, StepId asStepId) {
        firstAsStepMap.put(element, asStepId);
    }

    public StepId getAsStep(GraphElement element) {
        return firstAsStepMap.get(element);
    }

    public StepId getOrderStepId(GraphElement element) {
        return orderStepId.get(element);
    }

    public void addOrderStepId(GraphElement element, StepId stepId) {
        orderStepId.put(element, stepId);
    }
}
