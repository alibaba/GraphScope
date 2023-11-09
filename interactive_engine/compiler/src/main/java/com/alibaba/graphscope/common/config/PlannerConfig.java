package com.alibaba.graphscope.common.config;

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

    private final boolean isOn;
    private final Opt opt;
    private final List<String> rules;
    private final int glogueSize;

    protected PlannerConfig(boolean isOn, Opt type, List<String> rules, int glogueSize) {
        this.isOn = isOn;
        this.opt = type;
        this.rules = Objects.requireNonNull(rules);
        this.glogueSize = glogueSize;
    }

    public static PlannerConfig create(Configs configs) {
        try {
            boolean isOn = GRAPH_PLANNER_IS_ON.get(configs);
            Opt type = Opt.valueOf(GRAPH_PLANNER_OPT.get(configs));
            List<String> ruleList = Utils.convertDotString(GRAPH_PLANNER_RULES.get(configs));
            return new PlannerConfig(
                    isOn, type, ruleList, GRAPH_PLANNER_CBO_GLOGUE_SIZE.get(configs));
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
