package com.alibaba.graphscope.ds;

import java.lang.Long;

public class Vertex_cxx_0xaccf3424Factory implements Vertex.Factory<Long> {
  public static final Vertex.Factory<Long> INSTANCE;

  static {
    INSTANCE = new Vertex_cxx_0xaccf3424Factory();
  }

  public Vertex_cxx_0xaccf3424Factory() {
  }

  public Vertex<Long> create() {
    return new Vertex_cxx_0xaccf3424(Vertex_cxx_0xaccf3424.nativeCreateFactory0());
  }
}
