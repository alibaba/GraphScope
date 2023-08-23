package com.alibaba.graphscope.common.config;

import com.google.common.collect.Lists;

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
            boolean isOn = GRAPH_PLANNER_IS_ON.get(configs);
            Opt type = Opt.valueOf(GRAPH_PLANNER_OPT.get(configs));
            String[] ruleNames = GRAPH_PLANNER_RULES.get(configs).split(",");
            List<String> ruleList = Lists.newArrayList();
            if (ruleNames != null && ruleNames.length > 0) {
                for (String ruleName : ruleNames) {
                    ruleList.add(ruleName.trim());
                }
            }
            return new PlannerConfig(isOn, type, ruleList);
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

    @Override
    public String toString() {
        return "PlannerConfig{" + "isOn=" + isOn + ", opt=" + opt + ", rules=" + rules + '}';
    }
}
