package com.alibaba.graphscope.groot.store.external;

import com.alibaba.maxgraph.common.config.Configs;
import java.io.IOException;
import java.net.URI;

public abstract class ExternalStorage {
    public static ExternalStorage getStorage(Configs configs, String path) throws IOException {
        URI uri = URI.create(path);
        String scheme = uri.getScheme();
        switch (scheme) {
            case "hdfs":
                return new HdfsStorage(path);
            case "oss":
                return new OssStorage(configs, path);
            default:
                throw new IllegalArgumentException(
                        "external storage scheme [" + scheme + "] not supported");
        }
    }

    public abstract void downloadData(String srcPath, String dstPath) throws IOException;
}
