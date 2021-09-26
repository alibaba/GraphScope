package com.alibaba.graphscope.utils;

import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

public class Gid2DataResizable implements Gid2Data{
    private int size;
    private ArrayList<Long> gids;
    private ArrayList<Writable> data;

    /**
     * Not resizable.
     * @param capacity
     */
    public Gid2DataResizable(int capacity){
        gids = new ArrayList<>(capacity);
        data = new ArrayList<>(capacity);
        size = 0;
    }

    public ArrayList<Long> getGids(){
        return gids;
    }

    public ArrayList<Writable> getData(){
        return data;
    }

    public boolean add(long gid, Writable writable){
        gids.add(gid);
        data.add(writable);
        size += 1;
        return true;
    }

    public void clear(){
        gids.clear();
        data.clear();
        size = 0;
    }
    public int size(){
        return size;
    }

    /**
     * Number of bytes need for serialization.
     *
     * @return number of butes
     */
    @Override
    public int serializedSize() {
        throw  new IllegalStateException("Not implemented intentionally");
    }
}
