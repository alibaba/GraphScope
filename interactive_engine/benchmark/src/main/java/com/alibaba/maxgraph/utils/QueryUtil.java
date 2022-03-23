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
package com.alibaba.maxgraph.utils;

import com.alibaba.maxgraph.common.*;

import java.util.ArrayList;
import java.util.List;

public class QueryUtil {
    public static List<LdbcQuery> initQueryList(Configuration configuration) throws Exception {
        String queryDir = configuration.getString(Configuration.QUERY_DIR);
        String parameterDir = configuration.getString(Configuration.PARAMETER_DIR);
        List<LdbcQuery> queryList = new ArrayList<>();

        for (int index = 1; index <= 100; index++) {
            String enableQuery = String.format("ldbc.snb.interactive.LdbcQuery%d_enable", index);
            String queryFileName = String.format("interactive-complex-%d.gremlin", index);
            String parameterFileName = String.format("interactive_%d_param.txt", index);
            if (configuration.getBoolean(enableQuery, false)) {
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
                } else if (index == 6) {
                    queryList.add(new LdbcQuery6(queryName, queryFilePath, parameterFilePath));
                } else if (index == 7) {
                    queryList.add(new LdbcQuery7(queryName, queryFilePath, parameterFilePath));
                } else if (index == 8) {
                    queryList.add(new LdbcQuery8(queryName, queryFilePath, parameterFilePath));
                } else if (index == 9) {
                    queryList.add(new LdbcQuery9(queryName, queryFilePath, parameterFilePath));
                } else if (index == 11) {
                    queryList.add(new LdbcQuery11(queryName, queryFilePath, parameterFilePath));
                } else if (index == 12) {
                    queryList.add(new LdbcQuery12(queryName, queryFilePath, parameterFilePath));
                } else {
                    queryList.add(new LdbcQuery(queryName, queryFilePath, parameterFilePath));
                }
            }
        }

        // for k hop
        for (int index = 1; index < 5; index++) {
            String enableQuery = String.format("ldbc.snb.interactive.%dhop_enable", index);
            String queryFileName = String.format("%d-hop.gremlin", index);
            String parameterFileName = String.format("%d_hop_param.txt", index);

            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("%d-hop", index);

                queryList.add(new HopQuery(queryName, queryFilePath, parameterFilePath));
            }
        }

        for (int index = 1; index < 100; index++) {
            String enableQuery =
                    String.format("ldbc.snb.interactive.query_%d_without_parameter_enable", index);
            String queryFileName = String.format("query_%d_without_parameter.gremlin", index);

            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String queryName = String.format("query_%d_without_parameter", index);

                queryList.add(new QueryWithoutParameter(queryName, queryFilePath));
            }
        }

        return queryList;
    }
}
