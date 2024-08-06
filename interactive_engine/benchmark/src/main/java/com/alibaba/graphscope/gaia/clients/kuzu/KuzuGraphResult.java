package com.alibaba.graphscope.gaia.clients.kuzu;

import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.kuzudb.KuzuFlatTuple;
import com.kuzudb.KuzuQueryResult;

public class KuzuGraphResult implements GraphResultSet {

    KuzuQueryResult result;

    public KuzuGraphResult(KuzuQueryResult result) {
        this.result = result;
    }

    public KuzuGraphResult() {
        this.result = null;
    }

    @Override
    public boolean hasNext() {
        if (result == null) {
            return false;
        }
        try {
            boolean hasNext = result.hasNext();
            if (!hasNext) {
                result.destroy();
            }
            return hasNext;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Object next() {
        if (result == null) {
            return null;
        }
        try {
            KuzuFlatTuple tuple = result.getNext();
            String tupleString = tuple.toString();
            tuple.destroy();
            return tupleString;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
