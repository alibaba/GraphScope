package com.alibaba.graphscope.loader;

import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface GraphDataBufferManager {

    void addVertex(int threadId, Writable id, Writable value) throws IOException;


    void addEdges(int threadId, Writable id, Iterable<Edge> edges)throws IOException;

    void addEdge(int threadId, WritableComparable srcId, WritableComparable dstId, Writable value)throws IOException;

    void reserveNumVertices(int length);

    void reserveNumEdges(int length);

    void finishAdding();
}
