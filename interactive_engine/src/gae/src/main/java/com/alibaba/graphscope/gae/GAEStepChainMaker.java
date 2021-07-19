package com.alibaba.graphscope.gae;

import com.alibaba.graphscope.gae.parser.GAE;
import com.alibaba.graphscope.gae.parser.GIE;
import com.alibaba.graphscope.gae.parser.Generator;
import com.alibaba.graphscope.gaia.JsonUtils;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Collections;
import java.util.Map;

public class GAEStepChainMaker implements Generator {
    @Override
    public Map<String, Object> generate(Map<String, Object> args) {
        String json = readFileFromResource("gae.template.json");
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
}
