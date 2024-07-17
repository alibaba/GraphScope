package com.alibaba.graphscope.gaia.clients.impls;

import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.kuzudb.KuzuQueryResult;

public class KuzuGraphResult implements GraphResultSet {

    KuzuQueryResult result;

    public KuzuGraphResult(KuzuQueryResult result) {
        this.result = result;
    }

    @Override
    public boolean hasNext() {
        try {
            return result.hasNext();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Object next() {
        try {
            return result.getNext();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
