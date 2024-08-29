package com.alibaba.graphscope.example.circle.parallel.formal;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liumin
 * @date 2024-07-26
 */
public class PathSerAndDeser {
    public static void serialize(DataOutput dataOutput, LongArrayList path) throws IOException {
        int vSize = path.size();
        if (vSize == 0) {
            System.out.println("serialize size is 0,path=[" + path + "]");
        }
        dataOutput.writeInt(vSize);
        for (LongCursor v : path) {
            dataOutput.writeLong(v.value);
        }
    }

    public static LongArrayList deserialize(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        if (size == 0) {
            System.out.println("deserialize size is 0");
        }

        LongArrayList list = new LongArrayList(size, CircleUtil.RESIZE_STRATEGY);
        for (int i = 0; i < size; i++) {
            list.add(dataInput.readLong());
        }
        return list;
    }
}
