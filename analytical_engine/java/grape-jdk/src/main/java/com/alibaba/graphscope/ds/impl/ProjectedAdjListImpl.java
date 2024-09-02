package com.alibaba.graphscope.ds.impl;

import com.alibaba.graphscope.ds.ProjectedAdjList;
import com.alibaba.graphscope.ds.ProjectedNbr;
import java.util.Iterator;

public class ProjectedAdjListImpl<VID_T,EDATA_T> implements ProjectedAdjList<VID_T, EDATA_T> {

    private ProjectedNbr<VID_T, EDATA_T> begin;
    private ProjectedNbr<VID_T, EDATA_T> end;
    private int elementSize;

    public ProjectedAdjListImpl(ProjectedNbr<VID_T, EDATA_T> begin, ProjectedNbr<VID_T, EDATA_T> end) {
        this.begin = begin;
        this.end = end;
        //If VID_T is long, elementSize is 16, otherwise 8
        elementSize = 16;
    }

    public ProjectedNbr<VID_T, EDATA_T> begin() {
        return begin;
    }

    public ProjectedNbr<VID_T, EDATA_T> end() {
        return end;
    }

    public long size() {
        return (end.getAddress() - begin.getAddress()) / elementSize;
    }

    public boolean empty() {
        return begin.eq(end);
    }

    public boolean notEmpty() {
        return !empty();
    }

    /**
     * The iterator for ProjectedAdjList. You can use enhanced for loop instead of directly using
     * this.
     *
     * @return the iterator.
     */
    public Iterable<ProjectedNbr<VID_T, EDATA_T>> iterable() {
        return () ->
            new Iterator<ProjectedNbr<VID_T, EDATA_T>>() {
                ProjectedNbr<VID_T, EDATA_T> cur = begin().dec();
                ProjectedNbr<VID_T, EDATA_T> end = end();
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
                public ProjectedNbr<VID_T, EDATA_T> next() {
                    flag = false;
                    return cur;
                }
            };
    }
}
