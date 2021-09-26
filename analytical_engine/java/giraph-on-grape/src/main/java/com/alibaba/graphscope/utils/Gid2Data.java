package com.alibaba.graphscope.utils;

import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

public interface Gid2Data {

    boolean add(long gid, Writable writable);

    void clear();

    int size();

    /**
     * Number of bytes need for serialization.
     * @return number of butes
     */
    int serializedSize();

    static Gid2Data newResizable(int capacity){
        return (Gid2Data) new Gid2DataResizable(capacity);
    }

    static Gid2Data newFixed(int capacity){
        return (Gid2Data) new Gid2DataFixed(capacity);
    }


}
