package com.alibaba.maxgraph.v2.frontend.compiler.plan.edge;

public class PartitionEdge implements Comparable<PartitionEdge> {
    private final int index;
    private PartitionType partitionType;
    private ShuffleType shuffleType;

    public PartitionEdge(PartitionType partitionType) {
        this(partitionType, 0);
    }

    public PartitionEdge(PartitionType partitionType, int index) {
        this.index = index;
        this.partitionType = partitionType;
    }

    public int getIndex() {
        return index;
    }

    public PartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(PartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public ShuffleType getShuffleType() {
        return shuffleType;
    }

    public void setShuffleType(ShuffleType shuffleType) {
        this.shuffleType = shuffleType;
    }

    @Override
    public int compareTo(PartitionEdge that) {
        if (index < that.index) {
            return -1;
        } else if (index > that.index) {
            return 1;
        } else {
            return 0;
        }
    }
}
