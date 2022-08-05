package com.alibaba.graphscope.groot.store.external;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsStorage extends ExternalStorage {

    private FileSystem fs;

    public HdfsStorage(String path) throws IOException {
        Path dataDir = new Path(path);
        Configuration conf = new Configuration();
        this.fs = dataDir.getFileSystem(conf);
    }

    @Override
    public void downloadData(String srcPath, String dstPath) throws IOException {
        fs.copyToLocalFile(new Path(srcPath), new Path(dstPath));
    }
}
