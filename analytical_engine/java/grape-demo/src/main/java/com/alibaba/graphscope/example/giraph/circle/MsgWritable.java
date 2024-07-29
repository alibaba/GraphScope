/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.graphscope.example.giraph.circle;

import com.alibaba.fastjson.JSONObject;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
        return size > 1 && (Long) vertexList.get(0) == (Long) vertexList.get(size - 1);
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

        while (var3.hasNext()) {
            long v = (Long) var3.next();
            dataOutput.writeLong(v);
        }

        int eSize = this.edgePath.size();
        dataOutput.writeInt(eSize);
        Iterator var8 = this.edgePath.iterator();

        while (var8.hasNext()) {
            long e = (Long) var8.next();
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
            for (int i = 0; i < size; ++i) {
                list.add(dataInput.readLong());
            }
        }

        return list;
    }

    @Override
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
            MsgWritable other = (MsgWritable) otherObj;
            return ((String)
                                    this.vertexPath.stream()
                                            .map(
                                                    (i) -> {
                                                        return i + "";
                                                    })
                                            .collect(Collectors.joining(",")))
                            .equals(
                                    other.vertexPath.stream()
                                            .map(
                                                    (i) -> {
                                                        return i + "";
                                                    })
                                            .collect(Collectors.joining(",")))
                    && ((String)
                                    this.edgePath.stream()
                                            .map(
                                                    (i) -> {
                                                        return i + "";
                                                    })
                                            .collect(Collectors.joining(",")))
                            .equals(
                                    other.edgePath.stream()
                                            .map(
                                                    (i) -> {
                                                        return i + "";
                                                    })
                                            .collect(Collectors.joining(",")));
        }
    }

    public int hashCode() {
        return Objects.hash(
                new Object[] {
                    this.vertexPath.stream()
                            .map(
                                    (i) -> {
                                        return i + "";
                                    })
                            .collect(Collectors.joining(",")),
                    this.edgePath.stream()
                            .map(
                                    (i) -> {
                                        return i + "";
                                    })
                            .collect(Collectors.joining(","))
                });
    }
}
