package com.alibaba.graphscope.example.traverse;

import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;

public class Traverse
        implements ParallelAppBase<Long, Long, Double, Long, TraverseContext>, ParallelEngine {

    @Override
    public void PEval(
            IFragment<Long, Long, Double, Long> fragment,
            ParallelContextBase<Long, Long, Double, Long> context,
            ParallelMessageManager messageManager) {
        TraverseContext ctx = (TraverseContext) context;
        for (Vertex<Long> vertex : fragment.innerVertices()) {
            AdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterator()) {
                Vertex<Long> dst = nbr.neighbor();
                // Update largest distance for current vertex
                ctx.vertexArray.setValue(vertex, Math.max(nbr.data(), ctx.vertexArray.get(vertex)));
            }
        }
        messageManager.ForceContinue();
    }

    @Override
    public void IncEval(
            IFragment<Long, Long, Double, Long> fragment,
            ParallelContextBase<Long, Long, Double, Long> context,
            ParallelMessageManager messageManager) {
        TraverseContext ctx = (TraverseContext) context;
        for (Vertex<Long> vertex : fragment.innerVertices()) {
            AdjList<Long, Long> adjList = fragment.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterator()) {
                Vertex<Long> dst = nbr.neighbor();
                // Update largest distance for current vertex
                ctx.vertexArray.setValue(vertex, Math.max(nbr.data(), ctx.vertexArray.get(vertex)));
            }
        }
    }
}
