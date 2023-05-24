package com.alibaba.graphscope.groot.store.external;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HdfsStorage extends ExternalStorage {
    private static final Logger logger = LoggerFactory.getLogger(HdfsStorage.class);

    private FileSystem fs;

    public HdfsStorage(String path) throws IOException {
        Path dataDir = new Path(path);
        Configuration conf = new Configuration();
        this.fs = dataDir.getFileSystem(conf);
    }

    @Override
    public void downloadDataSimple(String srcPath, String dstPath) throws IOException {
        if (fs.exists(new Path(srcPath))) {
            fs.copyToLocalFile(new Path(srcPath), new Path(dstPath));
        } else {
            logger.warn("Path doesn't exists: " + srcPath);
        }
    }

    public void downloadData(String srcPath, String dstPath) throws IOException {
        downloadDataSimple(srcPath, dstPath);
    }
}
