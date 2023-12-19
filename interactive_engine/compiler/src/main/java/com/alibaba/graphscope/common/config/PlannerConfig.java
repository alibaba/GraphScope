package com.alibaba.graphscope.common.config;

import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

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

    private final boolean isOn;
    private final Opt opt;
    private final List<String> rules;
    private final int glogueSize;
    private final GlogueSchema glogueSchema;

    protected PlannerConfig(
            boolean isOn, Opt type, List<String> rules, int glogueSize, GlogueSchema glogueSchema) {
        this.isOn = isOn;
        this.opt = type;
        this.rules = Objects.requireNonNull(rules);
        this.glogueSize = glogueSize;
        this.glogueSchema = glogueSchema;
    }

    public static PlannerConfig create(Configs configs) {
        try {
            boolean isOn = GRAPH_PLANNER_IS_ON.get(configs);
            Opt type = Opt.valueOf(GRAPH_PLANNER_OPT.get(configs));
            List<String> ruleList = Utils.convertDotString(GRAPH_PLANNER_RULES.get(configs));
            String schemaPath = GRAPH_PLANNER_CBO_GLOGUE_SCHEMA.get(configs);
            GlogueSchema schema =
                    schemaPath.isEmpty()
                            ? new GlogueSchema().DefaultGraphSchema()
                            : new GlogueSchema().SchemaFromFile(schemaPath);
            return new PlannerConfig(
                    isOn, type, ruleList, GRAPH_PLANNER_CBO_GLOGUE_SIZE.get(configs), schema);
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

    public int getGlogueSize() {
        return glogueSize;
    }

    public GlogueSchema getGlogueSchema() {
        return glogueSchema;
    }

    @Override
    public String toString() {
        return "PlannerConfig{"
                + "isOn="
                + isOn
                + ", opt="
                + opt
                + ", rules="
                + rules
                + ", glogueSize="
                + glogueSize
                + '}';
    }
}
