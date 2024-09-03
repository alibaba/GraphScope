package com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern;

import java.util.Objects;

public class PathExpandRange implements Comparable<PathExpandRange> {
    private final int offset;
    private final int fetch;

    public PathExpandRange(int offset, int fetch) {
        this.offset = offset;
        this.fetch = fetch;
    }

    public int getOffset() {
        return offset;
    }

    public int getFetch() {
        return fetch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PathExpandRange that = (PathExpandRange) o;
        return offset == that.offset && fetch == that.fetch;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, fetch);
    }

    @Override
    public int compareTo(PathExpandRange o) {
        if (this.offset != o.offset) {
            return this.offset - o.offset;
        }
        return this.fetch - o.fetch;
    }
}
