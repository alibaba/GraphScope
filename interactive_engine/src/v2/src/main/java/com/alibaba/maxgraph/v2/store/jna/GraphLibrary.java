package com.alibaba.maxgraph.v2.store.jna;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public interface GraphLibrary extends Library {
    GraphLibrary INSTANCE = Native.load("maxgraph_store", GraphLibrary.class);

    Pointer openGraphStore(byte[] config, int len);
    boolean closeGraphStore(Pointer pointer);
    JnaResponse writeBatch(Pointer pointer, long snapshotId, byte[] data, int len);
    JnaResponse getGraphDefBlob(Pointer pointer);
    JnaResponse ingestData(Pointer pointer, String dataPath);
    void dropJnaResponse(JnaResponse jnaResponse);

}
