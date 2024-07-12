package com.alibaba.graphscope.example.circle;

import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.context.DefaultContextBase;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.DefaultMessageManager;
import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIByteVectorFactory;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class CirclePIE implements DefaultAppBase<
        Long,
        Long,
        Long,
        Long,
        CirclePIEContext> {
    private static final Logger logger = LoggerFactory.getLogger(CirclePIE.class);
    private static FFIByteVectorOutputStream msgVector = new FFIByteVectorOutputStream();

    /**
     * Partial Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    @Override
    public void PEval(IFragment<Long, Long, Long, Long> graph, DefaultContextBase<Long, Long, Long, Long> context, DefaultMessageManager messageManager) {
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        CirclePIEContext ctx = (CirclePIEContext) context;
        for (long i = 0; i < graph.getInnerVerticesNum(); ++i) {
            vertex.setValue(i);
            Long globalId = graph.getInnerVertexGid(vertex);
            Path path = new Path(globalId);
            AdjList<Long, Long> adjList = graph.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterable()) {
                path.add(graph.vertex2Gid(nbr.neighbor()));
                if (graph.isOuterVertex(nbr.neighbor())) {
                    // send path to outer vertex.
                    try {
                        sendMessageToOuterVertex(graph, messageManager, nbr.neighbor(), path);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    ctx.addToNextPath(nbr.neighbor(), path);
                }
                path.pop();
            }
        }
        //No need to check circels
        ctx.swapPaths();
        logger.info("After PEval: cur_path: " + ctx.curPaths.toString());
        logger.info("After PEval: next_path: " + ctx.nextPaths.toString());
        messageManager.forceContinue();
    }

    /**
     * Incremental Evaluation to implement.
     *
     * @param graph          fragment. The graph fragment providing accesses to graph data.
     * @param context        context. User defined context which manages data during the whole
     *                       computations.
     * @param messageManager The message manger which manages messages between fragments.
     * @see IFragment
     * @see DefaultContextBase
     * @see DefaultMessageManager
     */
    @Override
    public void IncEval(IFragment<Long, Long, Long, Long> graph, DefaultContextBase<Long, Long, Long, Long> context, DefaultMessageManager messageManager) {
        CirclePIEContext ctx = (CirclePIEContext) context;
        //Receive msg and merge
        try {
            receiveMessage(graph, messageManager, ctx);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("In super step {}, cur path {}", ctx.curStep, ctx.curPaths);
        logger.info("In super step {}, next path {}", ctx.curStep, ctx.nextPaths);
        // For received msg, check if it is already circle, if true, add to the final results.
        ctx.persistCirclePathInCurrent();

        // Implement vertex program
        vprog();

        if (ctx.curStep < ctx.maxStep - 1){
            // send msg
            sendMessageThroughOE(graph, ctx, messageManager);
            ctx.swapPaths();
        }
        else if (ctx.curStep == ctx.maxStep - 1){
            // check whether received paths start with the nbr.
            // No work
            messageManager.forceContinue();
        } else {
            // maybe receive message, but not sending message.
            logger.info("Max step reached, " + ctx.curStep);
        }
    }

    void sendMessageThroughOE(IFragment<Long,Long,Long,Long> graph, CirclePIEContext ctx, DefaultMessageManager messageManager) {
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        for (long i = 0; i < graph.getInnerVerticesNum(); ++i) {
            vertex.setValue(i);
            Long globalId = graph.getInnerVertexGid(vertex);
            List<Path> paths = ctx.curPaths.get((int) i);
            for (int j = 0; j < paths.size(); ++j) {
                Path path = paths.get(j);
                //Check whether the last node is exactly current vertex.
                if (path.top() != globalId){
                    logger.error("Invalid path, ending at {}, but collected by {}", path.top(), globalId);
                }
                AdjList<Long, Long> adjList = graph.getOutgoingAdjList(vertex);
                for (Nbr<Long, Long> nbr : adjList.iterable()) {
                    path.add(graph.vertex2Gid(nbr.neighbor()));
                    if (graph.isOuterVertex(nbr.neighbor())) {
                        // send path to outer vertex.
                        try {
                            sendMessageToOuterVertex(graph, messageManager, nbr.neighbor(), path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        ctx.addToNextPath(nbr.neighbor(), path);
                    }
                    path.pop();
                }
            }
        }
    }

    /**
     * Send a message to the outer vertex (to other fragment)
     * @param neighbor the outer vertex vid
     */
    void sendMessageToOuterVertex(IFragment<Long, Long, Long, Long> graph, DefaultMessageManager mm, Vertex<Long> neighbor, Path path) throws IOException {
        logger.info("Send path {} to vertex {}, dst frag {}", path, neighbor.getValue(), graph.fid());
        msgVector.reset();
        msgVector.writeLong(graph.getOuterVertexGid(neighbor));
        path.write(msgVector);
        mm.sendToFragment(graph.getFragId(neighbor), msgVector.getVector());
    }

    void receiveMessage(IFragment<Long, Long, Long, Long> graph, DefaultMessageManager messageManager, CirclePIEContext ctx)throws IOException {
        FFIByteVector tmpVector = (FFIByteVector) FFIByteVectorFactory.INSTANCE.create();
        long bytesOfReceivedMsg = 0;
        Vertex<Long> tmpVertex = FFITypeFactoryhelper.newVertexLong();
        while (messageManager.getPureMessage(tmpVector)) {
            // The retrieved tmp vector has been resized, so the cached objAddress is not available.
            // trigger the refresh
            tmpVector.touch();
            bytesOfReceivedMsg += tmpVector.size();
            logger.info("Frag [{}] digest message of size {}", graph.fid(), tmpVector.size());
            Path path = new Path();
            FFIByteVectorInputStream inputStream = new FFIByteVectorInputStream(tmpVector);
            long gid = inputStream.readLong();
            if (!graph.innerVertexGid2Vertex(gid, tmpVertex)){
                logger.error("Fail to get lid from gid {}", gid);
            }
            logger.info("Got msg to lid {}", tmpVertex.getValue());
            path.read(inputStream);
            // Add the tail node of new path here.
//            path.add(gid);
            digestMessage(ctx, tmpVertex, path);
            tmpVector.clear();
        }
        logger.info("total message received by frag {} bytes {}", graph.fid(), bytesOfReceivedMsg);
        tmpVector.delete();
    }

    void digestMessage(CirclePIEContext ctx, Vertex<Long> vertex, Path path ) {
        ctx.addToCurrentPath(vertex,path);
    }

    void vprog() {

    }
}
