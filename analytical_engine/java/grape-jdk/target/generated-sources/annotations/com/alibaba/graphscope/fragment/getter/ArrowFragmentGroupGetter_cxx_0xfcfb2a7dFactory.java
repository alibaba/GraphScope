package com.alibaba.graphscope.fragment.getter;

public class ArrowFragmentGroupGetter_cxx_0xfcfb2a7dFactory implements ArrowFragmentGroupGetter.Factory {
  public static final ArrowFragmentGroupGetter.Factory INSTANCE;

  static {
    INSTANCE = new ArrowFragmentGroupGetter_cxx_0xfcfb2a7dFactory();
  }

  public ArrowFragmentGroupGetter_cxx_0xfcfb2a7dFactory() {
  }

  public ArrowFragmentGroupGetter create() {
    return new ArrowFragmentGroupGetter_cxx_0xfcfb2a7d(ArrowFragmentGroupGetter_cxx_0xfcfb2a7d.nativeCreateFactory0());
  }
}
