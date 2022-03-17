package com.alibaba.graphscope.common.store;

import com.alibaba.graphscope.common.jna.IrCoreLibrary;

import java.util.Optional;

public abstract class IrMetaFetcher {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    protected abstract Optional<String> getIrMeta();

    public void fetch() {
        Optional<String> irMetaOpt = getIrMeta();
        if (irMetaOpt.isPresent()) {
            irCoreLib.setSchema(irMetaOpt.get());
        } else {
            throw new RuntimeException("ir meta is not ready, retry please");
        }
    }
}
