/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.util;

import com.alibaba.maxgraph.sdkcommon.client.Endpoint;
import com.alibaba.maxgraph.common.client.SimpleWorkerInfo;
import com.alibaba.maxgraph.sdkcommon.util.HTTPUtils;
import com.alibaba.maxgraph.sdkcommon.util.JSON;
import com.alibaba.maxgraph.proto.RoleType;
import com.alibaba.maxgraph.proto.WorkerStatus;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.util.List;
import java.util.stream.Collectors;

public class WebAPIUtils {
    private static final String GET_WORKER_INFO_API = "/openApi/instances/%s/endpoint";

    public static List<SimpleWorkerInfo> getAllWorkers(String webUrl, String graphName) throws Exception {
        CloseableHttpResponse response = HTTPUtils.sendGetRequest(webUrl + String.format(GET_WORKER_INFO_API, graphName));
        String responseContent = HTTPUtils.getResponseContent(response);
        List<SimpleWorkerInfo> workerInfoList = JSON.parseAsList(responseContent, SimpleWorkerInfo.class);
        return workerInfoList;
    }


    public static List<Endpoint> getFrontendWorkers(String webUrl, String graphName) throws Exception {
        List<SimpleWorkerInfo> workerInfoList = getAllWorkers(webUrl, graphName);
        return workerInfoList.stream()
                .filter(workerInfo -> workerInfo.roleType == RoleType.FRONTEND
                        && workerInfo.workerStatus == WorkerStatus.RUNNING)
                .map(timeServerInfo -> timeServerInfo.endpoint)
                .collect(Collectors.toList());
    }
}
