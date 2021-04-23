package com.alibaba.maxgraph.v2.common.util;

public class PartitionUtils {
    public static int getPartitionIdFromKey(long partitionKey, int partitionCount) {
        return (int) Math.floorMod(partitionKey, (long) partitionCount);
    }
}
