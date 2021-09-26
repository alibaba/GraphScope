package com.alibaba.graphscope.utils;

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

//TODO: Gid2Data<GS_VID_T,M>
public class Gid2DataFixed implements Gid2Data{
    private int size;
    private long [] gids;
    private Writable[] data;

    /**
     * Not resizable.
     * @param capacity
     */
    public Gid2DataFixed(int capacity){
        gids = new long[capacity];
        data = new Writable[capacity];
        size = 0;
    }

    public long [] getGids(){
        return gids;
    }

    public Writable[] getMsgOnVertex(){
        return data;
    }

    public boolean add(long gid, Writable writable){
        if (size == gids.length){
            return false;
        }
        else {
            gids[size] = gid;
            data[size++] = writable;
            return true;
        }
    }

    public void clear(){
        //release objs
        for (int i = 0; i < size; ++i){
            data[i] = null;
        }
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
        //minimum size of writable is 0.
        return size * (4 + 0);
    }

    public void write(DataOutput output) throws IOException {
        output.writeInt(size);
        for (int i = 0 ; i < size; ++i){
            output.writeLong(gids[i]);
            data[i].write(output);
        }
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("Gid2DataFix(size=" + size);
        sb.append(",gids=");
        sb.append(gids[0]).append("...").append(data[0]).append("...)");
        return sb.toString();
    }
}
