package com.alibaba.maxgraph.v2.frontend.compiler.rpc;

import com.alibaba.maxgraph.v2.frontend.graph.SnapshotMaxGraph;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraverser;
import org.apache.tinkerpop.gremlin.server.Context;

public class MaxGraphTraverserResultProcessor extends MaxGraphGremlinResultProcessor {
    public MaxGraphTraverserResultProcessor(Context context, int resultIterationSize, SnapshotMaxGraph snapshotMaxGraph) {
        super(context, resultIterationSize, snapshotMaxGraph);
    }

    @Override
    public Object transformResult(Object result) {
        if (result instanceof RemoteTraverser) {
            return result;
        } else {
            return new DefaultRemoteTraverser(result, 1);
        }
    }
}
