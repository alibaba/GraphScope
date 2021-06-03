/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
