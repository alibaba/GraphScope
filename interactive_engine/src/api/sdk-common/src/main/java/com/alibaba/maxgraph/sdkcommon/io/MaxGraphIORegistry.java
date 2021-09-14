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
package com.alibaba.maxgraph.sdkcommon.io;

import com.alibaba.maxgraph.sdkcommon.compiler.custom.CustomPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.ListKeyPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.ListMatchType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.ListPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.Lists;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.MatchType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.NegatePredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.PredicateType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.RegexKeyPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.RegexPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.StringKeyPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.StringPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.Text;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimMatchType;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimPredicate;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.dim.DimTable;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.map.MapPropFillFunction;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.map.Prop;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.output.OutputTable;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.program.ConnectedComponentVertexProgram;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.program.CustomVertexProgram;
import com.alibaba.maxgraph.sdkcommon.compiler.custom.program.Program;
import com.alibaba.maxgraph.sdkcommon.graph.CompositeId;
import com.alibaba.maxgraph.sdkcommon.graph.DfsQueryRequest;
import com.alibaba.maxgraph.sdkcommon.graph.EntryValueResult;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

public class MaxGraphIORegistry extends AbstractIoRegistry {
    private static final MaxGraphIORegistry INSTANCE = new MaxGraphIORegistry();

    public MaxGraphIORegistry() {
        register(GryoIo.class, CompositeId.class, null);
        register(GryoIo.class, DfsQueryRequest.class, null);
        register(GryoIo.class, EntryValueResult.class, null);
        register(GryoIo.class, CustomPredicate.class, null);
        register(GryoIo.class, ListKeyPredicate.class, null);
        register(GryoIo.class, ListMatchType.class, null);
        register(GryoIo.class, ListPredicate.class, null);
        register(GryoIo.class, Lists.class, null);
        register(GryoIo.class, MatchType.class, null);
        register(GryoIo.class, NegatePredicate.class, null);
        register(GryoIo.class, PredicateType.class, null);
        register(GryoIo.class, RegexKeyPredicate.class, null);
        register(GryoIo.class, RegexPredicate.class, null);
        register(GryoIo.class, StringKeyPredicate.class, null);
        register(GryoIo.class, StringPredicate.class, null);
        register(GryoIo.class, Text.class, null);
        register(GryoIo.class, DimMatchType.class, null);
        register(GryoIo.class, DimPredicate.class, null);
        register(GryoIo.class, DimTable.class, null);
        register(GryoIo.class, OutputTable.class, null);
        register(GryoIo.class, Prop.class, null);
        register(GryoIo.class, MapPropFillFunction.class, null);
        register(GryoIo.class, Program.class, null);
        register(GryoIo.class, ConnectedComponentVertexProgram.class, null);
        register(GryoIo.class, CustomVertexProgram.class, null);
    }

    public static MaxGraphIORegistry instance() {
        return INSTANCE;
    }
}
