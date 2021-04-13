package com.alibaba.graphscope.gaia.plan.extractor;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum PropertyExtractorFactory implements PropertyExtractor {
    /**
     * has("name", "xxx").has("age", 20) will be extracted as ["name", "age"]
     */
    Has {
        @Override
        public List<String> extractProperties(Step step) {
            List<String> properties = new ArrayList<>();
            List<HasContainer> containerList = ((HasStep) step).getHasContainers();
            for (HasContainer container : containerList) {
                if (!container.getKey().equals(T.id.getAccessor()) && !container.getKey().equals(T.label.getAccessor())) {
                    properties.add(container.getKey());
                }
            }
            return properties;
        }
    },
    /**
     * values("name", "age") will be extracted as ["name", "age"]
     */
    Values {
        @Override
        public List<String> extractProperties(Step step) {
            List<String> properties = new ArrayList<>();
            PropertiesStep step1 = (PropertiesStep) step;
            if (step1.getPropertyKeys() != null) {
                properties.addAll(Arrays.asList(step1.getPropertyKeys()));
            }
            return properties;
        }
    },
    /**
     * valueMap("name", "age") will be extracted as ["name", "age"]
     */
    ValueMap {
        @Override
        public List<String> extractProperties(Step step) {
            List<String> properties = new ArrayList<>();
            PropertyMapStep step1 = (PropertyMapStep) step;
            if (step1.getPropertyKeys() != null) {
                properties.addAll(Arrays.asList(step1.getPropertyKeys()));
            }
            return properties;
        }
    }
}
