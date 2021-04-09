package com.alibaba.maxgraph.v2.ingestor;

import java.util.List;

public interface IngestProgressFetcher {
    List<Long> getTailOffsets(List<Integer> queueIds);
}
