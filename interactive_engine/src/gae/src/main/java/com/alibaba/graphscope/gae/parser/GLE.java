package com.alibaba.graphscope.gae.parser;

import com.alibaba.graphscope.gaia.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkperpop.gremlin.groovy.custom.SampleStep;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public enum GLE implements Generator {
    SAMPLE {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            Traversal traversal = getTraversal(args);
            String graphName = getGraphName(args);
            String json = readFileFromResource("gle.sample.json");
            Map<String, Object> sample = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
            });
            sample.put("graph", graphName);
            SampleStep sampleStep = getSampleStep(traversal);
            if (sampleStep != null) {
                sample.put("params", Collections.singletonMap("format", sampleStep.getFormat()));
                return sample;
            } else {
                throw new UnsupportedOperationException("");
            }
        }

        private SampleStep getSampleStep(Traversal traversal) {
            List<Step> steps = traversal.asAdmin().getSteps();
            for (Step step : steps) {
                if (step instanceof SampleStep) {
                    return (SampleStep) step;
                }
            }
            return null;
        }
    },
    TensorFlow {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            String graphName = getGraphName(args);
            String json = readFileFromResource("gle.tensorflow.json");
            Map<String, Object> tensorFlow = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
            });
            tensorFlow.put("graph", graphName);
            tensorFlow.put("operation", "to_tensorflow");
            return tensorFlow;
        }
    }
}
