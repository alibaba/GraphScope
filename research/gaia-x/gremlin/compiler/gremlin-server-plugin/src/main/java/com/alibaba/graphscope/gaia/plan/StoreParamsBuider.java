package com.alibaba.graphscope.gaia.plan;

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.Gremlin;

import java.util.List;
import java.util.Map;

public class StoreParamsBuider {
    private Gremlin.QueryParams.Builder builder;

    public static StoreParamsBuider newBuilder() {
        return new StoreParamsBuider();
    }

    private StoreParamsBuider() {
        this.builder = Gremlin.QueryParams.newBuilder();
    }

    public StoreParamsBuider setGraphLabels(List<String> graphLabels) {
        Gremlin.QueryParams.Labels.Builder labels = Gremlin.QueryParams.Labels.newBuilder();
        if (!graphLabels.isEmpty()) {
            graphLabels.forEach(l -> labels.addLabels(Integer.valueOf(l)));
        }
        this.builder.setLabels(labels);
        return this;
    }

    public StoreParamsBuider setRequiredProperties(Gremlin.PropKeys properties) {
        this.builder.setRequiredProperties(properties);
        return this;
    }

    public StoreParamsBuider setLimit(int limit) {
        this.builder.setLimit(Gremlin.QueryParams.Limit.newBuilder().setLimit(limit).build());
        return this;
    }

    public StoreParamsBuider setPredicates(Gremlin.FilterChain chain) {
        this.builder.setPredicates(chain);
        return this;
    }

    public StoreParamsBuider setExtraParams(Map<String, Common.Value> values) {
        Gremlin.QueryParams.ExtraParams.Builder params = Gremlin.QueryParams.ExtraParams.newBuilder();
        values.forEach((k, v) -> {
            params.addParams(Gremlin.QueryParams.ExtraParams.Params.newBuilder()
                    .setKey(k)
                    .setValue(v)
                    .build());
        });
        this.builder.setExtraParams(params);
        return this;
    }

    public Gremlin.QueryParams build() {
        return this.builder.build();
    }
}
