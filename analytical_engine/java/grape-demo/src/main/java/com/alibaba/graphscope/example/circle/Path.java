package com.alibaba.graphscope.example.circle;

import com.alibaba.graphscope.serialization.FFIByteVectorInputStream;
import com.alibaba.graphscope.serialization.FFIByteVectorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Stack;

public class Path {
    private static final Logger logger = LoggerFactory.getLogger(Path.class);
    private Stack<Long> vertexInnerIds;

    public Path() {
        vertexInnerIds = new Stack<>();
    }

    public Path(Path path) {
        vertexInnerIds = new Stack<>();
        vertexInnerIds.addAll(path.vertexInnerIds);
    }

    public Path(long vid) {
        vertexInnerIds = new Stack<>();
        vertexInnerIds.add(vid);
    }

    public void add(long vid) {
        vertexInnerIds.add(vid);
    }

    public boolean isCircle() {
        if (vertexInnerIds.size() <= 2){
            return false;
        }
        if (vertexInnerIds.peek().equals(vertexInnerIds.get(0))){
            return true;
        }
        return false;
    }

    public long top() {
        return vertexInnerIds.peek();
    }

    public void pop() {
        vertexInnerIds.pop();
    }

    public void write(FFIByteVectorOutputStream output) throws IOException {
        output.writeInt(vertexInnerIds.size());
        for (int i = 0; i < vertexInnerIds.size(); ++i) {
            output.writeLong(vertexInnerIds.get(i));
        }
    }

    public void read(FFIByteVectorInputStream input) throws IOException {
        if (vertexInnerIds.size() != 0) {
            throw new RuntimeException("The Path is not empty");
        }
        int len = input.readInt();
        // logger.info("reading {} elements from stream", len);
        for (int i = 0; i < len; ++i) {
            vertexInnerIds.push(input.readLong());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Path{");
        for (int i = 0; i < vertexInnerIds.size(); ++i){
            sb.append(vertexInnerIds.get(i));
            if (i < vertexInnerIds.size() - 1){
                sb.append(",");
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
