package com.alibaba.graphscope.gaia.plan.meta;

import com.alibaba.graphscope.gaia.plan.meta.object.GraphElement;
import com.alibaba.graphscope.gaia.plan.meta.object.StepId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropertiesMapMeta extends DefaultMapMeta<GraphElement, StepPropertiesMeta> {
    private Map<GraphElement, StepId> firstAsStepMap = new HashMap<>();

    @Override
    public void add(GraphElement object, StepPropertiesMeta data) {
        List<String> properties = data.getProperties();
        if (properties != null && !properties.isEmpty()) {
            if (this.mapMeta.get(object) == null) {
                this.mapMeta.put(object, new StepPropertiesMeta(new ArrayList<>(data.getProperties()), data.getStepId(), data.getStep()));
            } else {
                this.mapMeta.get(object).addProperties(data.getProperties());
            }
        }
    }

    public void addAsStep(GraphElement element, StepId asStepId) {
        firstAsStepMap.put(element, asStepId);
    }

    public StepId getAsStep(GraphElement element) {
        return firstAsStepMap.get(element);
    }
}
