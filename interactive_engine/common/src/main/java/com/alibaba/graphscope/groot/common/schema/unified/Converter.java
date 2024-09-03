package com.alibaba.graphscope.groot.common.schema.unified;

import com.fasterxml.jackson.databind.util.StdConverter;

public class Converter extends StdConverter<Graph, Graph> {
    @Override
    public Graph convert(Graph var1) {
        var1.schema = var1.schema.munge();
        return var1;
    }
}
