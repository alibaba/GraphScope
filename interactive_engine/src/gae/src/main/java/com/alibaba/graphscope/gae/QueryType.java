package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gae.parser.GAE;
import com.alibaba.graphscope.gae.parser.GIE;
import com.alibaba.graphscope.gaia.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkperpop.gremlin.groovy.custom.StringProcessStep;
import org.apache.tinkperpop.gremlin.groovy.custom.TraversalProcessStep;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public enum QueryType implements QueryChainMaker {
    PAGE_RANK {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            String json = readFileFromResource("query/pagerank.json");
            Map<String, Object> chain = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
            });
            Map step1 = GAE.RUN_APP.generate(args);
            step1.put("deps", Collections.emptyList());
            chain.put("step-1", step1);

            Map step2 = GAE.Add_Column.generate(args);
            step2.put("deps", "step-1");
            chain.put("step-2", step2);

            Map step3 = GIE.GREMLIN_QUERY.generate(args);
            step3.put("deps", "step-2");
            chain.put("step-3", step3);
            return chain;
        }

        @Override
        public boolean isValid(Traversal query) {
            List<Step> steps = query.asAdmin().getSteps();
            for (Step step : steps) {
                if (step instanceof TraversalProcessStep) {
                    return true;
                }
            }
            return false;
        }
    },
    SSSP {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            String json = readFileFromResource("query/sssp.json");
            Map<String, Object> chain = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
            });
            Map step1 = GAE.RUN_APP.generate(args);
            step1.put("deps", Collections.emptyList());
            chain.put("step-1", step1);
            return chain;
        }

        @Override
        public boolean isValid(Traversal query) {
            List<Step> steps = query.asAdmin().getSteps();
            for (Step step : steps) {
                if (step instanceof StringProcessStep) {
                    return true;
                }
            }
            return false;
        }
    },
    GRAPH_LEARN {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            throw new UnsupportedOperationException("");
        }

        @Override
        public boolean isValid(Traversal query) {
            return false;
        }
    }
}
