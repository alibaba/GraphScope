package com.alibaba.maxgraph.v2.common.frontend.api.io;

import com.alibaba.maxgraph.v2.common.frontend.result.CompositeId;
import com.alibaba.maxgraph.v2.common.frontend.result.EdgeResult;
import com.alibaba.maxgraph.v2.common.frontend.result.EntryValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PathValueResult;
import com.alibaba.maxgraph.v2.common.frontend.result.PropertyResult;
import com.alibaba.maxgraph.v2.common.frontend.result.VertexPropertyResult;
import com.alibaba.maxgraph.v2.common.frontend.result.VertexResult;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoSerializersV3d0;

/**
 * Maxgraph io registry, register maxgraph related class to kryo
 */
public class MaxGraphIORegistry extends AbstractIoRegistry {
    private static final MaxGraphIORegistry INSTANCE = new MaxGraphIORegistry();

    private MaxGraphIORegistry() {
        register(GryoIo.class, CompositeId.class, null);
        register(GryoIo.class, VertexResult.class, null);
        register(GryoIo.class, EdgeResult.class, null);
        register(GryoIo.class, VertexPropertyResult.class, null);
        register(GryoIo.class, PropertyResult.class, null);
        register(GryoIo.class, PathResult.class, null);
        register(GryoIo.class, PathValueResult.class, null);
        register(GryoIo.class, EntryValueResult.class, null);
        register(GryoIo.class, Text.class, null);
        register(GryoIo.class, AndP.class, new GryoSerializersV3d0.PSerializer());
        register(GryoIo.class, OrP.class, new GryoSerializersV3d0.PSerializer());
    }

    public static MaxGraphIORegistry instance() {
        return INSTANCE;
    }
}
