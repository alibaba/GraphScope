package com.alibaba.graphscope.groot.dataload.util;

import com.aliyun.odps.mapred.conf.JobConf;

import java.io.IOException;

public class FSFactory {
    public FSFactory() {}

    public static AbstractFileSystem Create(JobConf conf) throws IOException {
        String dataSinkType = conf.get(Constants.DATA_SINK_TYPE, "VOLUME");
        if (dataSinkType.equalsIgnoreCase("VOLUME")) {
            return new VolumeFS(conf);
        } else if (dataSinkType.equalsIgnoreCase("OSS")) {
            return new OSSFS(conf);
        } else if (dataSinkType.equalsIgnoreCase("HDFS")) {
            throw new IOException("HDFS as a data sink is not supported in ODPS");
        } else {
            throw new IOException("Unsupported data sink: " + dataSinkType);
        }
    }
}
