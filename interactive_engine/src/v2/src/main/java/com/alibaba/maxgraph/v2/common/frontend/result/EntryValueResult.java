package com.alibaba.maxgraph.v2.common.frontend.result;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Map;

/**
 * <Key, Value> format results in maxgraph
 */
public class EntryValueResult implements QueryResult, Map.Entry {
    private Object key;
    private Object value;

    public EntryValueResult() {
    }

    public EntryValueResult(Object key, Object value) {
        this.key = key;
        this.value = value;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public Object setValue(Object value) {
        Object tmp = this.value;
        this.value = value;
        return tmp;
    }

    public void setValue(QueryResult value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EntryValueResult that = (EntryValueResult) o;
        return Objects.equal(key, that.key) &&
                Objects.equal(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key, value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("value", value)
                .toString();
    }

    @Override
    public Object convertToGremlinStructure() {
        Object key = this.key instanceof QueryResult ? ((QueryResult) this.key).convertToGremlinStructure() : this.key;
        Object value = this.value instanceof QueryResult ? ((QueryResult) this.value).convertToGremlinStructure() : this.value;

        return new EntryValueResult(key, value);
    }
}
