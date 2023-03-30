package com.alibaba.graphscope.common.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class PlannerConfig {
    private static final String GRAPH_PLANNER = "graph.planner";
    private static final PlannerConfig DEFAULT =
            new PlannerConfig(false, Opt.RBO, Lists.newArrayList());

    private final boolean isOn;
    private final Opt opt;
    private final List<String> rules;

    protected PlannerConfig(boolean isOn, Opt type, List<String> rules) {
        this.isOn = isOn;
        this.opt = type;
        this.rules = Objects.requireNonNull(rules);
    }

    public static PlannerConfig create(Configs configs) {
        try {
            String json = configs.get(GRAPH_PLANNER, "");
            JsonNode rootNode = (new ObjectMapper()).readTree(json);
            if (rootNode == null || rootNode.isEmpty()) {
                return PlannerConfig.DEFAULT;
            } else {
                JsonNode node1 = rootNode.get("isOn");
                boolean isOn = (node1 == null) ? false : node1.asBoolean();
                node1 = rootNode.get("opt");
                Opt type = (node1 == null) ? Opt.RBO : Opt.valueOf(node1.asText());
                node1 = rootNode.get("rules");
                List<String> ruleNames = Lists.newArrayList();
                if (node1 != null && node1.isArray()) {
                    Iterator<JsonNode> ruleIt = node1.iterator();
                    while (ruleIt.hasNext()) {
                        ruleNames.add(ruleIt.next().asText());
                    }
                }
                return new PlannerConfig(isOn, type, ruleNames);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public enum Opt {
        RBO,
        CBO
    }

    public boolean isOn() {
        return isOn;
    }

    public Opt getOpt() {
        return opt;
    }

    public List<String> getRules() {
        return Collections.unmodifiableList(rules);
    }
}
