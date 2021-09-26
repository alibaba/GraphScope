package org.apache.giraph.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

public class JobContextImpl extends JobContext {

    public JobContextImpl(Configuration conf,
        JobID jobId) {
        super(conf, jobId);
    }
}
