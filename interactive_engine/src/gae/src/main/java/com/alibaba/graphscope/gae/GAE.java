package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gaia.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkperpop.gremlin.groovy.custom.TraversalProcessStep;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public enum GAE implements Generator {
    Add_Column {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            try {
                Traversal traversal = getGremlinQuery(args);
                String graphName = getGraphName(args);
                List<String> withParams = getWithParams(traversal);
                String json = readFileFromResource("gae.add.column.json");
                Map<String, Object> addColumn = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
                });
                addColumn.put("graph", graphName);
                Map params = (Map) addColumn.get("params");
                params.put("new_column_name", withParams.get(1));
                params.put("use_data", String.format("r:%s.%s", getLabel(traversal), withParams.get(0)));
                return addColumn;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private List<String> getWithParams(Traversal traversal) {
            List<Step> steps = traversal.asAdmin().getSteps();
            for (Step step : steps) {
                if (step instanceof TraversalProcessStep) {
                    String srcColumn = ((TraversalProcessStep) step).getSrcColumn();
                    srcColumn = (srcColumn.charAt(0) == '$') ? srcColumn.substring(1) : srcColumn;
                    String dstColumn = ((TraversalProcessStep) step).getDstColumn();
                    return Arrays.asList(srcColumn, dstColumn);
                }
            }
            return Collections.emptyList();
        }

        private String getLabel(Traversal traversal) {
            List<Step> steps = traversal.asAdmin().getSteps();
            for (Step step : steps) {
                if (step instanceof HasStep) {
                    List<HasContainer> containers = ((HasStep) step).getHasContainers();
                    if (!containers.isEmpty()) {
                        HasContainer hasContainer = containers.get(0);
                        if (hasContainer.getKey().equals(T.label.getAccessor())) {
                            return (String) hasContainer.getValue();
                        }
                    }
                }
            }
            return "_V";
        }
    },
    RUN_APP {
        @Override
        public Map<String, Object> generate(Map<String, Object> args) {
            String graphName = getGraphName(args);
            String json = readFileFromResource("gae.run.app.json");
            Map<String, Object> runApp = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
            });
            String srcCode = readFileFromResource("src");
            runApp.put("graph", graphName);
            Map params = (Map) runApp.get("params");
            params.put("cpp_code", srcCode);
            return runApp;
        }
    }
}
