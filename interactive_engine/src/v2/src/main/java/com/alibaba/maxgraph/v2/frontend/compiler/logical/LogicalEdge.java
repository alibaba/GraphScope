package com.alibaba.maxgraph.v2.frontend.compiler.logical;

import com.alibaba.maxgraph.v2.frontend.compiler.logical.edge.EdgeShuffleType;
import com.google.common.base.MoreObjects;

public class LogicalEdge {
    private EdgeShuffleType shuffleType;
    private long shuffleConstant;
    private int shufflePropId;
    private int streamIndex = 0;

    private LogicalEdge(EdgeShuffleType shuffleType,
                        long shuffleConstant,
                        int shufflePropId,
                        int streamIndex) {
        this.shuffleType = shuffleType;
        this.shuffleConstant = shuffleConstant;
        this.shufflePropId = shufflePropId;
        this.streamIndex = streamIndex;
    }

    public LogicalEdge() {
        this(EdgeShuffleType.SHUFFLE_BY_KEY);
    }

    public LogicalEdge(EdgeShuffleType shuffleType) {
        this.shuffleType = shuffleType;
        this.shuffleConstant = 0;
    }

    public EdgeShuffleType getShuffleType() {
        return shuffleType;
    }

    public long getShuffleConstant() {
        return shuffleConstant;
    }

    public int getShufflePropId() {
        return shufflePropId;
    }

    public void setStreamIndex(int streamIndex) {
        this.streamIndex = streamIndex;
    }

    public static LogicalEdge shuffleByKey(int shufflePropId) {
        return new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_KEY, 0, shufflePropId, 0);
    }

    public static LogicalEdge forwardEdge() {
        return new LogicalEdge(EdgeShuffleType.FORWARD);
    }

    public static LogicalEdge shuffleConstant() {
        return new LogicalEdge(EdgeShuffleType.SHUFFLE_BY_CONST, 0, 0, 0);
    }

    public void setShuffleTypeForward() {
        this.shuffleType = EdgeShuffleType.FORWARD;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("shuffleType", shuffleType)
                .add("shuffleConstant", shuffleConstant)
                .toString();
    }

    public int getStreamIndex() {
        return streamIndex;
    }
}
