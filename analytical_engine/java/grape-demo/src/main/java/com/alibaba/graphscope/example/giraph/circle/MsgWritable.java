//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.alibaba.graphscope.example.giraph.circle;

import com.alibaba.fastjson.JSONObject;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.io.Writable;

public class MsgWritable implements Writable {
    private List<Long> vertexPath;
    private List<Long> edgePath;

    public MsgWritable() {
        this.vertexPath = new ArrayList();
        this.edgePath = new ArrayList();
    }

    public MsgWritable(List<Long> vertexPath, List<Long> edgePath) {
        this.vertexPath = vertexPath;
        this.edgePath = edgePath;
    }

    public static boolean isCircle(List<Long> vertexList) {
        int size = vertexList.size();
        return size > 1 && (Long)vertexList.get(0) == (Long)vertexList.get(size - 1);
    }

    public List<Long> getVertexPath() {
        return this.vertexPath;
    }

    public void setVertexPath(List<Long> vertexPath) {
        this.vertexPath = vertexPath;
    }

    public List<Long> getEdgePath() {
        return this.edgePath;
    }

    public void setEdgePath(List<Long> edgePath) {
        this.edgePath = edgePath;
    }

    public void write(DataOutput dataOutput) throws IOException {
        int vSize = this.vertexPath.size();
        dataOutput.writeInt(vSize);
        Iterator var3 = this.vertexPath.iterator();

        while(var3.hasNext()) {
            long v = (Long)var3.next();
            dataOutput.writeLong(v);
        }

        int eSize = this.edgePath.size();
        dataOutput.writeInt(eSize);
        Iterator var8 = this.edgePath.iterator();

        while(var8.hasNext()) {
            long e = (Long)var8.next();
            dataOutput.writeLong(e);
        }

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.vertexPath = this.readLongList(dataInput);
        this.edgePath = this.readLongList(dataInput);
    }

    private List<Long> readLongList(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        List<Long> list = new ArrayList();
        if (size != 0) {
            for(int i = 0; i < size; ++i) {
                list.add(dataInput.readLong());
            }
        }

        return list;
    }

    public String toString() {
        JSONObject json = new JSONObject();
        json.put("v", this.vertexPath);
        json.put("e", this.edgePath);
        return json.toJSONString();
    }

    public boolean equals(Object otherObj) {
        if (!(otherObj instanceof MsgWritable)) {
            return false;
        } else {
            MsgWritable other = (MsgWritable)otherObj;
            return ((String)this.vertexPath.stream().map((i) -> {
                return i + "";
            }).collect(Collectors.joining(","))).equals(other.vertexPath.stream().map((i) -> {
                return i + "";
            }).collect(Collectors.joining(","))) && ((String)this.edgePath.stream().map((i) -> {
                return i + "";
            }).collect(Collectors.joining(","))).equals(other.edgePath.stream().map((i) -> {
                return i + "";
            }).collect(Collectors.joining(",")));
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.vertexPath.stream().map((i) -> {
            return i + "";
        }).collect(Collectors.joining(",")), this.edgePath.stream().map((i) -> {
            return i + "";
        }).collect(Collectors.joining(","))});
    }
}
