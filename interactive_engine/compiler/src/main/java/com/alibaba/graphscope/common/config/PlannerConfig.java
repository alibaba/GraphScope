package com.alibaba.graphscope.common.config;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import java.util.Collections;
import java.util.List;

public class PlannerConfig {
    public static final Config<Boolean> GRAPH_PLANNER_IS_ON =
            Config.boolConfig("graph.planner.is.on", false);
    public static final Config<String> GRAPH_PLANNER_OPT =
            Config.stringConfig("graph.planner.opt", "RBO");
    public static final Config<String> GRAPH_PLANNER_RULES =
            Config.stringConfig("graph.planner.rules", "");
    public static final Config<Integer> GRAPH_PLANNER_CBO_GLOGUE_SIZE =
            Config.intConfig("graph.planner.cbo.glogue.size", 3);
    public static final Config<String> GRAPH_PLANNER_CBO_GLOGUE_SCHEMA =
            Config.stringConfig("graph.planner.cbo.glogue.schema", "");
    public static final Config<Integer> JOIN_MIN_PATTERN_SIZE =
            Config.intConfig("graph.planner.join.min.pattern.size", 4);
    public static final Config<Integer> JOIN_COST_FACTOR_1 =
            Config.intConfig("graph.planner.join.cost.factor.1", 1);
    public static final Config<Integer> JOIN_COST_FACTOR_2 =
            Config.intConfig("graph.planner.join.cost.factor.2", 1);

    private final Configs configs;
    private final List<String> rules;
    private final GlogueSchema glogueSchema;

    public PlannerConfig(Configs configs) {
        this.configs = configs;
        this.rules = Utils.convertDotString(GRAPH_PLANNER_RULES.get(configs));
        String schemaPath = GRAPH_PLANNER_CBO_GLOGUE_SCHEMA.get(configs);
        this.glogueSchema =
                schemaPath.isEmpty()
                        ? new GlogueSchema().DefaultGraphSchema()
                        : new GlogueSchema().SchemaFromFile(schemaPath);
    }

    public enum Opt {
        RBO,
        CBO
    }

    public boolean isOn() {
        return GRAPH_PLANNER_IS_ON.get(configs);
    }

    public Opt getOpt() {
        return Opt.valueOf(GRAPH_PLANNER_OPT.get(configs));
    }

    public List<String> getRules() {
        return Collections.unmodifiableList(rules);
    }

    public int getGlogueSize() {
        return GRAPH_PLANNER_CBO_GLOGUE_SIZE.get(configs);
    }

    public GlogueSchema getGlogueSchema() {
        return glogueSchema;
    }

    public int getJoinMinPatternSize() {
        return JOIN_MIN_PATTERN_SIZE.get(configs);
    }

    public int getJoinCostFactor1() {
        return JOIN_COST_FACTOR_1.get(configs);
    }

    public int getJoinCostFactor2() {
        return JOIN_COST_FACTOR_2.get(configs);
    }

    @Override
    public String toString() {
        return "PlannerConfig{"
                + "isOn="
                + isOn()
                + ", opt="
                + getOpt()
                + ", rules="
                + rules
                + ", glogueSize="
                + getGlogueSize()
                + '}';
    }
}
