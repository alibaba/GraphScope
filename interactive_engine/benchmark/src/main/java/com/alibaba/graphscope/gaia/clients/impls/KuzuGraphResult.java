package com.alibaba.graphscope.gaia.clients.impls;

import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.kuzudb.KuzuFlatTuple;
import com.kuzudb.KuzuQueryResult;

public class KuzuGraphResult implements GraphResultSet {

    KuzuQueryResult result;

    public KuzuGraphResult(KuzuQueryResult result) {
        this.result = result;
    }

    @Override
    public boolean hasNext() {
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
