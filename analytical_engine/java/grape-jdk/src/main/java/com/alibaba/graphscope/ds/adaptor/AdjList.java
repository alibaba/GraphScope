package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.GrapeNbr;
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
        if (type().equals(GrapeAdjListAdaptor.TYPE)) {
            return new Iterable<Nbr<VID_T, EDATA_T>>() {
                public Iterator<Nbr<VID_T, EDATA_T>> iterator() {
                    return new Iterator<Nbr<VID_T, EDATA_T>>() {
                        GrapeNbr<VID_T, EDATA_T> beginPtr = (GrapeNbr<VID_T, EDATA_T>) begin();
                        GrapeNbr<VID_T, EDATA_T> endPtr = (GrapeNbr<VID_T, EDATA_T>) end();
                        long currentAddress;
                        long endAddress;
                        long elementSize;

                        {
                            this.currentAddress = beginPtr.getAddress();
                            this.endAddress = endPtr.getAddress();
                            this.elementSize = beginPtr.elementSize();
                        }

                        public boolean hasNext() {
                            return this.currentAddress != this.endAddress;
                        }

                        public Nbr<VID_T, EDATA_T> next() {
                            beginPtr.moveToV(this.currentAddress);
                            this.currentAddress += this.elementSize;
                            return (Nbr<VID_T, EDATA_T>) beginPtr;
                        }
                    };
                }
            };
        } else if (type().equals(ProjectedAdjListAdaptor.TYPE)) {
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
        return null;
    }
}
