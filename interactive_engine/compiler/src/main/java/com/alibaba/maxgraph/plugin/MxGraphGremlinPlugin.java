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
package com.alibaba.maxgraph.plugin;

import com.alibaba.maxgraph.customizer.ASTSecurityCustomizer;
import com.alibaba.maxgraph.customizer.AnnotationCustomizer;
import com.alibaba.maxgraph.sdkcommon.meta.DataType;
import com.alibaba.maxgraph.sdkcommon.meta.DataTypeDeserializer;
import com.alibaba.maxgraph.sdkcommon.meta.DataTypeSerializer;
import com.alibaba.maxgraph.sdkcommon.meta.InternalDataType;
import com.alibaba.maxgraph.structure.DefaultProperty;
import com.alibaba.maxgraph.structure.MxDetachedPath;
import com.alibaba.maxgraph.structure.MxEdge;
import com.alibaba.maxgraph.structure.MxPath;
import com.alibaba.maxgraph.structure.MxVertex;
import com.alibaba.maxgraph.structure.MxVertexProperty;
import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;
import com.alibaba.maxgraph.tinkerpop.BigGraphFeatures;
import com.alibaba.maxgraph.tinkerpop.BigGraphVariables;
import com.alibaba.maxgraph.tinkerpop.steps.MxEdgeVertexStep;
import com.alibaba.maxgraph.tinkerpop.steps.MxGraphStep;
import com.alibaba.maxgraph.tinkerpop.strategies.MxGraphStepStrategy;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.mortbay.component.Container.Relationship;

public class MxGraphGremlinPlugin extends AbstractGremlinPlugin {
    private static final String NAME = "Alibaba.MaxGraph";
    private static final MxGraphGremlinPlugin instance = new MxGraphGremlinPlugin();

    private static final ImportCustomizer imports = DefaultImportCustomizer.build()
        .addClassImports(BigGraphFeatures.class,
            BigGraphVariables.class,
            DefaultProperty.class,
            MxDetachedPath.class,
            MxEdge.class,
            MxGraphGremlinPlugin.class,
            MxPath.class,
            MxVertex.class,
            MxVertexProperty.class,
            TinkerMaxGraph.class,
            MxEdgeVertexStep.class,
            MxGraphStep.class,
            MxGraphStepStrategy.class,
            DataTypeDeserializer.class,
            DataTypeSerializer.class,
            InternalDataType.class,
            Relationship.class,
            DataType.class).create();

    public MxGraphGremlinPlugin() {
        super(NAME, imports, new ASTSecurityCustomizer(), new AnnotationCustomizer());
    }

    @Override
    public String getName() {
        return NAME;
    }

    public static MxGraphGremlinPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
