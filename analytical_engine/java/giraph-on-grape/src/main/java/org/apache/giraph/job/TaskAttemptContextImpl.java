package org.apache.giraph.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class TaskAttemptContextImpl extends TaskAttemptContext {
    private String outputFileName;

    public TaskAttemptContextImpl(Configuration conf,
        TaskAttemptID taskId) {
        super(conf, taskId);
    }

    public void setOutputFileName(String outputFileName){
        this.outputFileName = outputFileName;
    }

    public String getOutputFileName(){
        return this.outputFileName;
    }
}
