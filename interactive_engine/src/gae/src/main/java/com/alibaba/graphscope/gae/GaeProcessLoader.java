package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gaia.plan.PlanUtils;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;

import java.util.Collections;

public class GaeProcessLoader {
    public static void load() {
        try {
            PlanUtils.setFinalStaticField(OpLoader.class, "processors", Collections.singletonMap("gae", new GaeOpProcessor()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
