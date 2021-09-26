/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Helpers for dealing with Hadoop.
 */
public class HadoopUtils {

    /**
     * Don't construct
     */
    private HadoopUtils() {}

    /**
     * Create a TaskAttemptContext, supporting many Hadoops.
     *
     * @param conf          Configuration
     * @param taskAttemptID TaskAttemptID to use
     * @return TaskAttemptContext
     */
    public static TaskAttemptContext makeTaskAttemptContext(
            Configuration conf, TaskAttemptID taskAttemptID) {
        TaskAttemptContext context;
        /*if[HADOOP_NON_JOBCONTEXT_IS_INTERFACE]
            context = new TaskAttemptContext(conf, taskAttemptID);
        else[HADOOP_NON_JOBCONTEXT_IS_INTERFACE]*/
        context = new TaskAttemptContextImpl(conf, taskAttemptID);
        /*end[HADOOP_NON_JOBCONTEXT_IS_INTERFACE]*/
        return context;
    }

    /**
     * Create a TaskAttemptContext, supporting many Hadoops.
     *
     * @param conf               Configuration
     * @param taskAttemptContext Use TaskAttemptID from this object
     * @return TaskAttemptContext
     */
    public static TaskAttemptContext makeTaskAttemptContext(
            Configuration conf, TaskAttemptContext taskAttemptContext) {
        return makeTaskAttemptContext(conf, taskAttemptContext.getTaskAttemptID());
    }

    /**
     * Create a TaskAttemptContext, supporting many Hadoops.
     *
     * @param conf Configuration
     * @return TaskAttemptContext
     */
    public static TaskAttemptContext makeTaskAttemptContext(Configuration conf) {
        return makeTaskAttemptContext(conf, new TaskAttemptID());
    }

    /**
     * Create a TaskAttemptContext, supporting many Hadoops.
     *
     * @return TaskAttemptContext
     */
    public static TaskAttemptContext makeTaskAttemptContext() {
        return makeTaskAttemptContext(new Configuration());
    }

    /**
     * Create a JobContext, supporting many Hadoops.
     *
     * @param conf  Configuration
     * @param jobID JobID to use
     * @return JobContext
     */
    public static JobContext makeJobContext(Configuration conf, JobID jobID) {
        JobContext context;
        /*if[HADOOP_NON_JOBCONTEXT_IS_INTERFACE]
            context = new JobContext(conf, jobID);
        else[HADOOP_NON_JOBCONTEXT_IS_INTERFACE]*/
        context = new JobContextImpl(conf, jobID);
        /*end[HADOOP_NON_JOBCONTEXT_IS_INTERFACE]*/
        return context;
    }

    /**
     * Get Job ID from job. May return null for hadoop 0.20.203
     *
     * @param job submitted job
     * @return JobId for submitted job.
     */
    public static JobID getJobID(Job job) {
        /*if[HADOOP_JOB_ID_AVAILABLE]
            return job.getID();
        else[HADOOP_JOB_ID_AVAILABLE]*/
        return job.getJobID();
        /*end[HADOOP_JOB_ID_AVAILABLE]*/
    }

    /**
     * Create a JobContext, supporting many Hadoops.
     *
     * @param conf       Configuration
     * @param jobContext Use JobID from this object
     * @return JobContext
     */
    public static JobContext makeJobContext(Configuration conf, JobContext jobContext) {
        return makeJobContext(conf, jobContext.getJobID());
    }

    /**
     * Create a JobContext, supporting many Hadoops.
     *
     * @param conf Configuration
     * @return JobContext
     */
    public static JobContext makeJobContext(Configuration conf) {
        return makeJobContext(conf, new JobID());
    }

    /**
     * Create a JobContext, supporting many Hadoops.
     *
     * @return JobContext
     */
    public static JobContext makeJobContext() {
        return makeJobContext(new Configuration());
    }
}
