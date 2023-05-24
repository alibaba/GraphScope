package com.alibaba.graphscope.groot.dataload.util;

import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;

import java.io.IOException;

public abstract class AbstractFileSystem implements AutoCloseable {

    protected AbstractFileSystem() {}

    public abstract void setJobConf(JobConf jobConf);

    public abstract void open(TaskContext context, String mode) throws IOException;

    public abstract String readToString(String fileName) throws IOException;

    public abstract void copy(String srcFile, String dstFile) throws IOException;

    @Override
    public void close() {}
}
