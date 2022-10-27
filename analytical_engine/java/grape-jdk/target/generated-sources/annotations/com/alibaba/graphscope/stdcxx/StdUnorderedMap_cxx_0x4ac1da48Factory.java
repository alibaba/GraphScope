package com.alibaba.graphscope.stdcxx;

import java.lang.Integer;
import java.lang.Long;

public class StdUnorderedMap_cxx_0x4ac1da48Factory implements StdUnorderedMap.Factory<Integer, Long> {
  public static final StdUnorderedMap.Factory<Integer, Long> INSTANCE;

  static {
    INSTANCE = new StdUnorderedMap_cxx_0x4ac1da48Factory();
  }

  public StdUnorderedMap_cxx_0x4ac1da48Factory() {
  }

  public StdUnorderedMap<Integer, Long> create() {
    return new StdUnorderedMap_cxx_0x4ac1da48(StdUnorderedMap_cxx_0x4ac1da48.nativeCreateFactory0());
  }
}
