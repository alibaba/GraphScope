package com.alibaba.graphscope.loader.impl;

import com.alibaba.graphscope.loader.GraphDataBufferManager;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIByteVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVector;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphDataBufferManangerImpl implements GraphDataBufferManager {

    private static Logger logger = LoggerFactory.getLogger(GraphDataBufferManangerImpl.class);
    private int threadNum;
    private int workerId;
    private FFIByteVecVector vidBuffers;
    private FFIByteVecVector vertexDataBuffers;
    private FFIByteVecVector edgeSrcIdBuffers;
    private FFIByteVecVector edgeDstIdBuffers;
    private FFIByteVecVector edgeDataBuffers;

    private FFIIntVecVector vidOffsets;
    private FFIIntVecVector vertexDataOffsets;
    private FFIIntVecVector edgeSrcIdOffsets;
    private FFIIntVecVector edgeDstIdOffsets;
    private FFIIntVecVector edgeDataOffsets;

    private FFIByteVectorOutputStream[] vidOutputStream;
    private FFIByteVectorOutputStream[] vdataOutputStream;
    private FFIByteVectorOutputStream[] edgeSrcIdOutputStream;
    private FFIByteVectorOutputStream[] edgeDstOutputStream;
    private FFIByteVectorOutputStream[] edgeDataOutStream;

    private FFIIntVector[] idOffsetsArr;
    private FFIIntVector[] vdataOffsetsArr;
    private FFIIntVector[] edgeSrcIdOffsetArr;
    private FFIIntVector[] edgeDstIdOffsetArr;
    private FFIIntVector[] edgeDataOffsetsArr;

    public GraphDataBufferManangerImpl(
        int workerId,
        int threadNum,
        FFIByteVecVector vidBuffers,
        FFIByteVecVector vertexDataBuffers,
        FFIByteVecVector edgeSrcIdBuffers,
        FFIByteVecVector edgeDstIdBuffers,
        FFIByteVecVector edgeDataBuffers,
        FFIIntVecVector vidOffsets,
        FFIIntVecVector vertexDataOffsets,
        FFIIntVecVector edgeSrcIdOffsets,
        FFIIntVecVector edgeDstIdOffsets,
        FFIIntVecVector edgeDataOffsets) {
        if (vidBuffers.getAddress() <= 0
            || vidOffsets.getAddress() <= 0
            || vertexDataBuffers.getAddress() <= 0
            || vertexDataOffsets.getAddress() <= 0
            || edgeSrcIdBuffers.getAddress() <= 0
            || edgeDstIdBuffers.getAddress() <= 0
            || edgeSrcIdOffsets.getAddress() <= 0
            || edgeDstIdOffsets.getAddress() <= 0
            || edgeDataBuffers.getAddress() <= 0
            || edgeDataOffsets.getAddress() <= 0) {
            throw new IllegalStateException("Empty buffer");
        }
        this.workerId = workerId;
        this.threadNum = threadNum;
        this.vidBuffers = vidBuffers;
        this.vertexDataBuffers = vertexDataBuffers;
        this.edgeSrcIdBuffers = edgeSrcIdBuffers;
        this.edgeDstIdBuffers = edgeDstIdBuffers;
        this.edgeDataBuffers = edgeDataBuffers;

        this.vidOffsets = vidOffsets;
        this.vertexDataOffsets = vertexDataOffsets;
        this.edgeSrcIdOffsets = edgeSrcIdOffsets;
        this.edgeDstIdOffsets = edgeDstIdOffsets;
        this.edgeDataOffsets = edgeDataOffsets;

        check();

        vidOutputStream = new FFIByteVectorOutputStream[threadNum];
        vdataOutputStream = new FFIByteVectorOutputStream[threadNum];
        edgeSrcIdOutputStream = new FFIByteVectorOutputStream[threadNum];
        edgeDstOutputStream = new FFIByteVectorOutputStream[threadNum];
        edgeDataOutStream = new FFIByteVectorOutputStream[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            vidOutputStream[i] = new FFIByteVectorOutputStream((FFIByteVector) vidBuffers.get(i));
            vdataOutputStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) vertexDataBuffers.get(i));
            edgeSrcIdOutputStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) edgeSrcIdBuffers.get(i));
            edgeDstOutputStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) edgeDstIdBuffers.get(i));
            edgeDataOutStream[i] =
                new FFIByteVectorOutputStream((FFIByteVector) edgeDataBuffers.get(i));
        }

        this.idOffsetsArr = new FFIIntVector[threadNum];
        this.vdataOffsetsArr = new FFIIntVector[threadNum];
        this.edgeSrcIdOffsetArr = new FFIIntVector[threadNum];
        this.edgeDstIdOffsetArr = new FFIIntVector[threadNum];
        this.edgeDataOffsetsArr = new FFIIntVector[threadNum];

        for (int i = 0; i < threadNum; ++i) {
            idOffsetsArr[i] = (FFIIntVector) vidOffsets.get(i);
            vdataOffsetsArr[i] = (FFIIntVector) vertexDataOffsets.get(i);
            edgeSrcIdOffsetArr[i] = (FFIIntVector) edgeSrcIdOffsets.get(i);
            edgeDstIdOffsetArr[i] = (FFIIntVector) edgeDstIdOffsets.get(i);
            edgeDataOffsetsArr[i] = (FFIIntVector) edgeDataOffsets.get(i);
        }
    }

    private void check() {
        if (vidBuffers.size() != threadNum) {
            vidBuffers.resize(threadNum);
        }
        if (vertexDataBuffers.size() != threadNum) {
            vertexDataBuffers.resize(threadNum);
        }
        if (edgeSrcIdBuffers.size() != threadNum) {
            edgeSrcIdBuffers.resize(threadNum);
        }
        if (edgeDstIdBuffers.size() != threadNum) {
            edgeDstIdBuffers.resize(threadNum);
        }
        if (edgeDataBuffers.size() != threadNum) {
            edgeDataBuffers.resize(threadNum);
        }

        if (vidOffsets.size() != threadNum) {
            vidOffsets.resize(threadNum);
        }
        if (vertexDataOffsets.size() != threadNum) {
            vertexDataOffsets.resize(threadNum);
        }
        if (edgeSrcIdOffsets.size() != threadNum) {
            edgeSrcIdOffsets.resize(threadNum);
        }
        if (edgeDstIdOffsets.size() != threadNum) {
            edgeDstIdOffsets.resize(threadNum);
        }
        if (edgeDataOffsets.size() != threadNum) {
            edgeDataOffsets.resize(threadNum);
        }
    }

    @Override
    public synchronized void addVertex(int threadId, Writable id, Writable value)
        throws IOException {
        int bytes = (int) -vidOutputStream[threadId].bytesWriten();
        id.write(vidOutputStream[threadId]);
        bytes += vidOutputStream[threadId].bytesWriten();
        idOffsetsArr[threadId].push_back(bytes);

        int bytes2 = (int) -vdataOutputStream[threadId].bytesWriten();
        value.write(vdataOutputStream[threadId]);
        bytes2 += vdataOutputStream[threadId].bytesWriten();
        vdataOffsetsArr[threadId].push_back(bytes2);

        // logger.debug(
        //     "adding vertex id {}, value {}, id offset {}, data offset {}, total bytes writen vid
        // buffer {}, vdata buffer {}",
        //     id, value, bytes, bytes2, vidOutputStream[threadId].bytesWriten(),
        //     vdataOutputStream[threadId].bytesWriten());
    }

    @Override
    public synchronized void addEdges(int threadId, Writable id, Iterable<Edge> edges)
        throws IOException {
        int bytesEdgeSrcOffset = 0, bytesEdgeDstOffset = 0, bytesDataOffsets = 0;

        for (Edge edge : edges) {
            bytesEdgeSrcOffset = (int) -edgeSrcIdOutputStream[threadId].bytesWriten();
            id.write(edgeSrcIdOutputStream[threadId]);
            bytesEdgeSrcOffset += edgeSrcIdOutputStream[threadId].bytesWriten();
            edgeSrcIdOffsetArr[threadId].push_back(bytesEdgeSrcOffset);

            bytesEdgeDstOffset = (int) -edgeDstOutputStream[threadId].bytesWriten();
            edge.getTargetVertexId().write(edgeDstOutputStream[threadId]);
            bytesEdgeDstOffset += edgeDstOutputStream[threadId].bytesWriten();
            edgeDstIdOffsetArr[threadId].push_back(bytesEdgeDstOffset);

            bytesDataOffsets = (int) -edgeDataOutStream[threadId].bytesWriten();
            edge.getValue().write(edgeDataOutStream[threadId]);
            bytesDataOffsets += edgeDataOutStream[threadId].bytesWriten();
            edgeDataOffsetsArr[threadId].push_back(bytesDataOffsets);

            // logger.debug("worker [{}] adding edge [{}]->[{}], value {}", workerId, id,
            // edge.getTargetVertexId(), edge.getValue());
        }
        // logger.debug("esrc/dst/data Total length : {} {} {}", bytesEdgeSrcOffset,
        // bytesEdgeDstOffset, bytesDataOffsets);
    }

    @Override
    public void addEdge(int threadId, WritableComparable srcId, WritableComparable dstId,
        Writable value)
        throws IOException {
        int bytesEdgeSrcOffset = 0, bytesEdgeDstOffset = 0, bytesDataOffsets = 0;

        bytesEdgeSrcOffset = (int) -edgeSrcIdOutputStream[threadId].bytesWriten();
        srcId.write(edgeSrcIdOutputStream[threadId]);
        bytesEdgeSrcOffset += edgeSrcIdOutputStream[threadId].bytesWriten();
        edgeSrcIdOffsetArr[threadId].push_back(bytesEdgeSrcOffset);

        bytesEdgeDstOffset = (int) -edgeDstOutputStream[threadId].bytesWriten();
        dstId.write(edgeDstOutputStream[threadId]);
        bytesEdgeDstOffset += edgeDstOutputStream[threadId].bytesWriten();
        edgeDstIdOffsetArr[threadId].push_back(bytesEdgeDstOffset);

        bytesDataOffsets = (int) -edgeDataOutStream[threadId].bytesWriten();
        value.write(edgeDataOutStream[threadId]);
        bytesDataOffsets += edgeDataOutStream[threadId].bytesWriten();
        edgeDataOffsetsArr[threadId].push_back(bytesDataOffsets);

        logger.debug("worker [{}] adding edge [{}]->[{}], value {}", workerId, srcId,
            dstId, value);
    }

    /**
     * resize for std vector
     *
     * @param length number of vertices.
     */
    @Override
    public void reserveNumVertices(int length) {
        int leastSize = length * 8;
        for (int i = 0; i < threadNum; ++i) {
            vidOutputStream[i].getVector().resize(leastSize);
            vidOutputStream[i].getVector().touch();
            vdataOutputStream[i].getVector().resize(leastSize);
            vdataOutputStream[i].getVector().touch();
//            edgeSrcIdOutputStream[i].getVector().resize(leastSize);
//            edgeSrcIdOutputStream[i].getVector().touch();
//            edgeDstOutputStream[i].getVector().resize(leastSize);
//            edgeDstOutputStream[i].getVector().touch();
//            edgeDataOutStream[i].getVector().resize(leastSize);
//            edgeDataOutStream[i].getVector().touch();

            idOffsetsArr[i].reserve(length); // may not enough
            idOffsetsArr[i].touch();
            vdataOffsetsArr[i].reserve(length);
            vdataOffsetsArr[i].touch();
//            edgeSrcIdOffsetArr[i].reserve(length);
//            edgeSrcIdOffsetArr[i].touch();
//            edgeDstIdOffsetArr[i].reserve(length);
//            edgeDstIdOffsetArr[i].touch();
//            edgeDataOffsetsArr[i].reserve(length);
//            edgeDataOffsetsArr[i].touch();
        }
    }

    @Override
    public void reserveNumEdges(int length) {
        int leastSize = length * 8;
        for (int i = 0; i < threadNum; ++i) {
            edgeSrcIdOutputStream[i].getVector().resizeWithMoreSpace(leastSize);
            edgeSrcIdOutputStream[i].getVector().touch();
            edgeDstOutputStream[i].getVector().resizeWithMoreSpace(leastSize);
            edgeDstOutputStream[i].getVector().touch();
            edgeDataOutStream[i].getVector().resizeWithMoreSpace(leastSize);
            edgeDataOutStream[i].getVector().touch();

            edgeSrcIdOffsetArr[i].reserve(length); //reserve since we push back
            edgeSrcIdOffsetArr[i].touch();
            edgeDstIdOffsetArr[i].reserve(length);
            edgeDstIdOffsetArr[i].touch();
            edgeDataOffsetsArr[i].reserve(length);
            edgeDataOffsetsArr[i].touch();
        }
    }

    @Override
    public void finishAdding() {
        for (int i = 0; i < threadNum; ++i) {
            vidOutputStream[i].finishSetting();
            vdataOutputStream[i].finishSetting();
            edgeSrcIdOutputStream[i].finishSetting();
            edgeDstOutputStream[i].finishSetting();
            edgeDataOutStream[i].finishSetting();

            idOffsetsArr[i].finishSetting();
            vdataOffsetsArr[i].finishSetting();
            edgeSrcIdOffsetArr[i].finishSetting();
            edgeDstIdOffsetArr[i].finishSetting();
            edgeDataOffsetsArr[i].finishSetting();
        }
    }
}
