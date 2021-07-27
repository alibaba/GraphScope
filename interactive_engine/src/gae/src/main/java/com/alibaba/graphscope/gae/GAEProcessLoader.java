package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

public class GAEProcessLoader {
    public static void load(InstanceConfig instanceConfig) {
        try {
            PlanUtils.setFinalStaticField(OpLoader.class, "processors",
                    ImmutableMap.of("gae", new GAEOpProcessor(instanceConfig),
                            "gae_traversal", new GAETraversalOpProcessor(instanceConfig)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
