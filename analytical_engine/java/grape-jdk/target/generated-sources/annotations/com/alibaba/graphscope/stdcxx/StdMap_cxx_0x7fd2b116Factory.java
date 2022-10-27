package com.alibaba.graphscope.stdcxx;

import com.alibaba.graphscope.graphx.Json;
import java.lang.Long;

public class StdMap_cxx_0x7fd2b116Factory implements StdMap.Factory<Long, Json> {
  public static final StdMap.Factory<Long, Json> INSTANCE;

  static {
    INSTANCE = new StdMap_cxx_0x7fd2b116Factory();
  }

  public StdMap_cxx_0x7fd2b116Factory() {
  }

  public StdMap<Long, Json> create() {
    return new StdMap_cxx_0x7fd2b116(StdMap_cxx_0x7fd2b116.nativeCreateFactory0());
  }
}
