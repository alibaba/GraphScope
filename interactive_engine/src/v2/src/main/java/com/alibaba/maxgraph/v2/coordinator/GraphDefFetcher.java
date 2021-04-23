package com.alibaba.maxgraph.v2.coordinator;

import com.alibaba.maxgraph.v2.common.rpc.RoleClients;
import com.alibaba.maxgraph.v2.common.schema.GraphDef;

public class GraphDefFetcher {

    private RoleClients<StoreSchemaClient> storeSchemaClients;

    public GraphDefFetcher(RoleClients<StoreSchemaClient> storeSchemaClients) {
        this.storeSchemaClients = storeSchemaClients;
    }

    public GraphDef fetchGraphDef() {
        return storeSchemaClients.getClient(0).fetchSchema();
    }
}
