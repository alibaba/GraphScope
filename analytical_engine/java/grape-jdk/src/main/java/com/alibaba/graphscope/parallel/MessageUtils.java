package com.alibaba.graphscope.parallel;

import com.alibaba.graphscope.fragment.adaptor.ArrowProjectedAdaptor;
import com.alibaba.graphscope.fragment.adaptor.ImmutableEdgecutFragmentAdaptor;
import com.alibaba.graphscope.utils.Unused;

public class MessageUtils {

    public static <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T> Unused getUnused(
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor,
            Class<? extends MSG_T> msgClass) {
        return Unused.getUnused(
                immutableAdaptor.getVdataClass(), immutableAdaptor.getEdataClass(), msgClass);
    }

    public static <OID_T, VID_T, VDATA_T, EDATA_T, MSG_T> Unused getUnused(
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> projectedAdaptor,
            Class<? extends MSG_T> msgClass) {
        return Unused.getUnused(
                projectedAdaptor.getVdataClass(), projectedAdaptor.getEdataClass(), msgClass);
    }

    public static <OID_T, VID_T, VDATA_T, EDATA_T> Unused getUnusedNoMsg(
            ImmutableEdgecutFragmentAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> immutableAdaptor) {
        return Unused.getUnused(immutableAdaptor.getVdataClass(), immutableAdaptor.getEdataClass());
    }

    public static <OID_T, VID_T, VDATA_T, EDATA_T> Unused getUnusedNoMsg(
            ArrowProjectedAdaptor<OID_T, VID_T, VDATA_T, EDATA_T> projectedAdaptor) {
        return Unused.getUnused(projectedAdaptor.getVdataClass(), projectedAdaptor.getEdataClass());
    }
}
