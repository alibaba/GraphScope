package com.alibaba.graphscope.gaia.plan.extractor;

import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
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
        public ToFetchProperties extractProperties(Step step) {
            List<String> properties = new ArrayList<>();
            List<HasContainer> containerList = ((HasStep) step).getHasContainers();
            for (HasContainer container : containerList) {
                if (!container.getKey().equals(T.id.getAccessor()) && !container.getKey().equals(T.label.getAccessor())) {
                    properties.add(container.getKey());
                }
            }
            return new ToFetchProperties(false, properties);
        }
    },
    /**
     * values("name", "age") will be extracted as ["name", "age"]
     */
    Values {
        @Override
        public ToFetchProperties extractProperties(Step step) {
            String[] properties = ((PropertiesStep) step).getPropertyKeys();
            boolean needAll = (properties == null || properties.length == 0) ? true : false;
            return new ToFetchProperties(needAll, Arrays.asList(properties));
        }
    },
    /**
     * valueMap("name", "age") will be extracted as ["name", "age"]
     */
    ValueMap {
        @Override
        public ToFetchProperties extractProperties(Step step) {
            String[] properties = ((PropertyMapStep) step).getPropertyKeys();
            boolean needAll = (properties == null || properties.length == 0) ? true : false;
            return new ToFetchProperties(needAll, Arrays.asList(properties));
        }
    }
}
