package com.alibaba.maxgraph.v2.frontend.server.plugin;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.common.schema.DataType;
import com.alibaba.maxgraph.v2.common.schema.PropertyDef;
import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import com.alibaba.maxgraph.v2.frontend.server.gremlin.variables.MaxGraphVariables;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

/**
 * MaxGraph plugin, used in server.yaml
 */
public class MaxGraphGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "Alibaba.MaxGraph";
    private static final MaxGraphGremlinPlugin INSTANCE = new MaxGraphGremlinPlugin();

    private static final ImportCustomizer IMPORTS = DefaultImportCustomizer.build()
            .addClassImports(MaxGraphVariables.class,
                    MaxGraphGremlinPlugin.class,
                    SnapshotMaxGraph.class,
                    EdgeType.class,
                    PropertyDef.class,
                    VertexType.class,
                    DataType.class).create();

    public MaxGraphGremlinPlugin() {
        super(NAME, IMPORTS, new ASTSecurityCustomizer(), new AnnotationCustomizer());
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static MaxGraphGremlinPlugin instance() {
        return INSTANCE;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
