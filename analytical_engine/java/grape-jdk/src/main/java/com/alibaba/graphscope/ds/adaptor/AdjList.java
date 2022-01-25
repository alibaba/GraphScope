package com.alibaba.graphscope.ds.adaptor;

import java.util.Iterator;
import java.util.Objects;

/**
 * This interface define the neighboring vertices and edge data for a vertex.
 *
 * @param <VID_T> vertex id type.
 * @param <EDATA_T> edge data type.
 */
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
     * The iterator for adjlist. You can use enhanced for loop instead of directly using this.
     *
     * @return the iterator.
     */
    default Iterable<Nbr<VID_T, EDATA_T>> iterator() {
        if (type().equals(GrapeAdjListAdaptor.TYPE)) {
            return () ->
                    new Iterator<Nbr<VID_T, EDATA_T>>() {
                        GrapeNbrAdaptor<VID_T, EDATA_T> beginPtr =
                                (GrapeNbrAdaptor<VID_T, EDATA_T>) begin();
                        GrapeNbrAdaptor<VID_T, EDATA_T> endPtr =
                                (GrapeNbrAdaptor<VID_T, EDATA_T>) end();
                        GrapeNbrAdaptor<VID_T, EDATA_T> curPtr =
                                (GrapeNbrAdaptor<VID_T, EDATA_T>) begin();
                        long currentAddress = 0;
                        long endAddress = 0;
                        long elementSize = 0;

                        // There are cases where begin() and end() nbrs are null.
                        {
                            if (Objects.nonNull(beginPtr.getGrapeNbr())
                                    && beginPtr.getAddress() > 0) {
                                this.currentAddress = beginPtr.getAddress();
                                this.elementSize = beginPtr.getGrapeNbr().elementSize();
                            }
                            if (Objects.nonNull(endPtr.getGrapeNbr()) && endPtr.getAddress() > 0) {
                                this.endAddress = endPtr.getAddress();
                            }
                        }

                        public boolean hasNext() {
                            return this.currentAddress != this.endAddress;
                        }

                        public Nbr<VID_T, EDATA_T> next() {
                            curPtr.setAddress(this.currentAddress);
                            this.currentAddress += this.elementSize;
                            return curPtr;
                        }
                    };
        } else if (type().equals(ProjectedAdjListAdaptor.TYPE)) {
            return () ->
                    new Iterator<Nbr<VID_T, EDATA_T>>() {
                        ProjectedNbrAdaptor<VID_T, EDATA_T> end =
                                (ProjectedNbrAdaptor<VID_T, EDATA_T>) end();
                        Nbr<VID_T, EDATA_T> curPtr = begin().dec();
                        boolean flag = false;

                        @Override
                        public boolean hasNext() {
                            if (!flag) {
                                curPtr = curPtr.inc();
                                flag = !curPtr.eq(end);
                            }
                            return flag;
                        }

                        @Override
                        public Nbr<VID_T, EDATA_T> next() {
                            flag = false;
                            return curPtr;
                        }
                    };
        }
        return null;
    }
}
