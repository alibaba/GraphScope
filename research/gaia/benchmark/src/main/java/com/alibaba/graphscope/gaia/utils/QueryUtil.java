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
package com.alibaba.graphscope.gaia.utils;

import com.alibaba.graphscope.gaia.common.*;

import java.util.ArrayList;
import java.util.List;

public class QueryUtil {
    public static List<CommonQuery> initQueryList(Configuration configuration) throws Exception {
        String queryDir = configuration.getString(Configuration.QUERY_DIR);
        String parameterDir = configuration.getString(Configuration.PARAMETER_DIR);
        List<CommonQuery> queryList = new ArrayList<>();

        // for ldbc queries
        for(int index = 1; index <= 100; index++) {
            String enableQuery = String.format("ldbc_query_%d.enable", index);
            String queryFileName = String.format("ldbc_query_%d.gremlin", index);
            String parameterFileName = String.format("ldbc_query_%d.param", index);
            if(configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("LDBC_QUERY_%d", index);
                if (index == 1) {
                    queryList.add(new LdbcQuery1(queryName, queryFilePath, parameterFilePath));
                } else if (index == 2) {
                    queryList.add(new LdbcQuery2(queryName, queryFilePath, parameterFilePath));
                } else if (index == 3) {
                    queryList.add(new LdbcQuery3(queryName, queryFilePath, parameterFilePath));
                } else if (index == 4) {
                    queryList.add(new LdbcQuery4(queryName, queryFilePath, parameterFilePath));
                } else if (index == 5) {
                    queryList.add(new LdbcQuery5(queryName, queryFilePath, parameterFilePath));
                } else if (index == 7) {
                    queryList.add(new LdbcQuery7(queryName, queryFilePath, parameterFilePath));
                } else if (index == 9) {
                    queryList.add(new LdbcQuery9(queryName, queryFilePath, parameterFilePath));
                } else if (index == 12) {
                    queryList.add(new LdbcQuery12(queryName, queryFilePath, parameterFilePath));
                } else {
                    queryList.add(new CommonQuery(queryName, queryFilePath, parameterFilePath));
                }
            }
        }

        // for k hop
        for(int index = 1; index < 5; index++) {
            String enableQuery = String.format("%d_hop_query.enable", index);
            String queryFileName = String.format("%d_hop_query.gremlin", index);
            String parameterFileName = String.format("%d_hop_query.param", index);

            if(configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("%d_HOP_QUERY", index);

                queryList.add(new CommonQuery(queryName, queryFilePath, parameterFilePath));
            }
        }

        // for benchmarking early-stop queries
        String enableEsQuery = "early_stop_query.enable";
        String esQueryFileName = "early_stop_query.gremlin";
        String esParameterFileName = "early_stop_query.param";
        if(configuration.getBoolean(enableEsQuery, false)) {
            String queryFilePath = String.format("%s/%s", queryDir, esQueryFileName);
            String parameterFilePath = String.format("%s/%s", parameterDir, esParameterFileName);
            String queryName = "EARLY_STOP_QUERY";
            queryList.add(new CommonQuery(queryName, queryFilePath, parameterFilePath));
        }

        // for benchmarking subtask queries
        String enableSubtaskQuery = "subtask_query.enable";
        String subtaskQueryFileName = "subtask_query.gremlin";
        String subtaskParameterFileName = "subtask_query.param";
        if(configuration.getBoolean(enableSubtaskQuery, false)) {
            String queryFilePath = String.format("%s/%s", queryDir, subtaskQueryFileName);
            String parameterFilePath = String.format("%s/%s", parameterDir, subtaskParameterFileName);
            String queryName = "SUBTASK_QUERY";
            queryList.add(new SubtaskQuery(queryName, queryFilePath, parameterFilePath));
        }

        // custom queries without parameter
        for(int index = 1; index < 100; index++) {
            String enableQuery = String.format("custom_constant_query_%d.enable", index);
            String queryFileName = String.format("custom_constant_query_%d.gremlin", index);

            if(configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String queryName = String.format("CUSTOM_QUERY_%d_WITHOUT_PARAMETER", index);

                queryList.add(new QueryWithoutParameter(queryName, queryFilePath));
            }
        }

        // custom queries
        for(int index = 1; index < 100; index++) {
            String enableQuery = String.format("custom_query_%d.enable", index);
            String queryFileName = String.format("custom_query_%d.gremlin", index);
            String parameterFileName = String.format("custom_query_%d.param", index);

            if(configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("CUSTOM_QUERY_%d", index);

                queryList.add(new CommonQuery(queryName, queryFilePath, parameterFilePath));
            }
        }

        return queryList;
    }
}
