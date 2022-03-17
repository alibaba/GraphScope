package com.alibaba.maxgraph.servers.ir;

import com.alibaba.maxgraph.common.config.Config;

public class SchemaConfig {
    public static final Config<String> SCHEMA_PATH =
            Config.stringConfig("graph.schema", "/usr/local/maxgraph/conf/ldbc.schema");
}
