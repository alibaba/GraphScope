package com.alibaba.graphscope.ds.adaptor;

import java.util.Iterator;

public interface AdjList<VID_T, EDATA_T> {
    String type();
    /**
     * Get the begin Nbr.
     *
     * @return the first Nbr.
     */
    Nbr<VID_T, EDATA_T> begin();

    /**
     * Get the last Nbr.
     *
     * @return the last Nbr.
     */
    Nbr<VID_T, EDATA_T> end();

    /**
     * Get the size of this adjList.
     *
     * @return size
     */
    long size();

    /**
     * The iterator for ProjectedAdjList. You can use enhanced for loop instead of directly using
     * this.
     *
     * @return the iterator.
     */
    default Iterable<Nbr<VID_T, EDATA_T>> iterator() {
        return () ->
                new Iterator<Nbr<VID_T, EDATA_T>>() {
                    Nbr<VID_T, EDATA_T> cur = begin().dec();
                    Nbr<VID_T, EDATA_T> end = end();
                    boolean flag = false;

                    @Override
                    public boolean hasNext() {
                        if (!flag) {
                            cur = cur.inc();
                            flag = !cur.eq(end);
                        }
                        return flag;
                    }

                    @Override
                    public Nbr<VID_T, EDATA_T> next() {
                        flag = false;
                        return cur;
                    }
                };
    }
}
