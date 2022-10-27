package com.alibaba.graphscope.graphx;

public class VineyardClient_cxx_0xb9ed250fFactory implements VineyardClient.Factory {
  public static final VineyardClient.Factory INSTANCE;

  static {
    INSTANCE = new VineyardClient_cxx_0xb9ed250fFactory();
  }

  public VineyardClient_cxx_0xb9ed250fFactory() {
  }

  public VineyardClient create() {
    return new VineyardClient_cxx_0xb9ed250f(VineyardClient_cxx_0xb9ed250f.nativeCreateFactory0());
  }
}
