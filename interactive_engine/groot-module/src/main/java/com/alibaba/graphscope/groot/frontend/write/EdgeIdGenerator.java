package com.alibaba.graphscope.groot.frontend.write;

import java.util.List;

public interface EdgeIdGenerator {

    long getNextId();

    long getHashId(long srcId, long dstId, int labelId, List<byte[]> pks);
}
