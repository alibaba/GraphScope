package com.alibaba.graphscope.interactive.client;

import com.alibaba.graphscope.interactive.client.common.Result;
import org.openapitools.client.model.ServiceStatus;

/**
 * Manage the query interface.
 */
public interface QueryServiceInterface {
    Result<ServiceStatus> getServiceStatus();

    Result<String> restartService();

    Result<String> startService();

    Result<String> stopService();
}
