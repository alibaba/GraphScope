package com.alibaba.graphscope.groot.store.external;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public abstract class ExternalStorage {
    public static ExternalStorage getStorage(String path, Map<String, String> configs)
            throws IOException {
        URI uri = URI.create(path);
        String scheme = uri.getScheme();
        switch (scheme) {
            case "hdfs":
                return new HdfsStorage(path);
            case "oss":
                return new OssStorage(path, configs);
            default:
                throw new IllegalArgumentException(
                        "external storage scheme [" + scheme + "] not supported");
        }
    }

    public abstract void downloadData(String srcPath, String dstPath) throws IOException;
}
