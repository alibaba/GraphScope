package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

import java.util.Collections;

public class GaeProcessLoader {
    public static void load(InstanceConfig instanceConfig) {
        try {
            PlanUtils.setFinalStaticField(OpLoader.class, "processors", Collections.singletonMap("gae", new GAEGremlinProcessor(instanceConfig)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
