package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import org.openapitools.client.model.JobStatus;

import java.util.List;

/**
 * Get/Cancel/List jobs.
 */
public interface JobInterface {
    Result<String> cancelJob(String jobId);

    Result<JobStatus> getJobStatus(String jobId);

    Result<List<JobStatus>> listJobs();
}
