//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.alibaba.graphscope.example.giraph.format;

import com.alibaba.graphscope.example.giraph.circle.MsgWritable;
import com.google.common.collect.Lists;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

public class VertexAttrWritable implements Writable {
    private List<MsgWritable> vertexAttr;

    public VertexAttrWritable() {
        this.vertexAttr = Lists.newArrayList(new MsgWritable[]{new MsgWritable()});
    }

    public VertexAttrWritable(List<MsgWritable> values) {
        this.vertexAttr = values;
    }

    public List<MsgWritable> getVertexAttr() {
        return this.vertexAttr;
    }

    public void setVertexAttr(List<MsgWritable> vertexAttr) {
        this.vertexAttr = vertexAttr;
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        List<MsgWritable> vertexAttr = new ArrayList();
        if (size != 0) {
            for(int i = 0; i < size; ++i) {
                MsgWritable msgWritable = new MsgWritable();
                msgWritable.readFields(in);
                vertexAttr.add(msgWritable);
            }
        }

        this.vertexAttr = vertexAttr;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.vertexAttr.size());
        Iterator var2 = this.vertexAttr.iterator();

        while(var2.hasNext()) {
            MsgWritable msgWritable = (MsgWritable)var2.next();
            msgWritable.write(out);
        }

    }

    public String toString() {
        List<String> pathList = (List)this.vertexAttr.stream().filter((path) -> {
            return MsgWritable.isCircle(path.getVertexPath());
        }).map((path) -> {
            return StringUtils.join(path.getEdgePath(), "&");
        }).collect(Collectors.toList());
        return !pathList.isEmpty() ? StringUtils.join(pathList, "|") : "";
    }
}
