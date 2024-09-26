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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class QueryUtil {
    private static final String LDBC_QUERY_1 = "ldbc_query_1";
    private static final String LDBC_QUERY_2 = "ldbc_query_2";
    private static final String LDBC_QUERY_3 = "ldbc_query_3";
    private static final String LDBC_QUERY_4 = "ldbc_query_4";
    private static final String LDBC_QUERY_5 = "ldbc_query_5";
    private static final String LDBC_QUERY_7 = "ldbc_query_7";
    private static final String LDBC_QUERY_9 = "ldbc_query_9";
    private static final String LDBC_QUERY_12 = "ldbc_query_12";
    private static final String BI_QUERY_1 = "bi_query_1";
    private static final String BI_QUERY_2 = "bi_query_2";
    private static final String BI_QUERY_8 = "bi_query_8";
    private static final String BI_QUERY_9 = "bi_query_9";
    private static final String BI_QUERY_11 = "bi_query_11";
    private static final String BI_QUERY_12 = "bi_query_12";

    private static CommonQuery buildCommonQuery(
            String queryName, String queryFileName, String paramFileName) throws Exception {
        if (paramFileName == null || paramFileName.isEmpty()) {
            return new QueryWithoutParameter(queryName, queryFileName);
        } else {
            if (LDBC_QUERY_1.equals(queryName.toLowerCase())) {
                return new LdbcQuery1(queryName, queryFileName, paramFileName);
            } else if (LDBC_QUERY_2.equals(queryName.toLowerCase())) {
                return new LdbcQuery2(queryName, queryFileName, paramFileName);
            } else if (LDBC_QUERY_3.equals(queryName.toLowerCase())) {
                return new LdbcQuery3(queryName, queryFileName, paramFileName);
            } else if (LDBC_QUERY_4.equals(queryName.toLowerCase())) {
                return new LdbcQuery4(queryName, queryFileName, paramFileName);
            } else if (LDBC_QUERY_5.equals(queryName.toLowerCase())) {
                return new LdbcQuery5(queryName, queryFileName, paramFileName);
            } else if (LDBC_QUERY_7.equals(queryName.toLowerCase())) {
                return new LdbcQuery7(queryName, queryFileName, paramFileName);
            } else if (LDBC_QUERY_9.equals(queryName.toLowerCase())) {
                return new LdbcQuery9(queryName, queryFileName, paramFileName);
            } else if (LDBC_QUERY_12.equals(queryName.toLowerCase())) {
                return new LdbcQuery12(queryName, queryFileName, paramFileName);
            } else if (BI_QUERY_1.equals(queryName.toLowerCase())) {
                return new BiQuery1(queryName, queryFileName, paramFileName);
            } else if (BI_QUERY_2.equals(queryName.toLowerCase())) {
                return new BiQuery2(queryName, queryFileName, paramFileName);
            } else if (BI_QUERY_8.equals(queryName.toLowerCase())) {
                return new BiQuery8(queryName, queryFileName, paramFileName);
            } else if (BI_QUERY_9.equals(queryName.toLowerCase())) {
                return new BiQuery9(queryName, queryFileName, paramFileName);
            } else if (BI_QUERY_11.equals(queryName.toLowerCase())) {
                return new BiQuery11(queryName, queryFileName, paramFileName);
            } else if (BI_QUERY_12.equals(queryName.toLowerCase())) {
                return new BiQuery12(queryName, queryFileName, paramFileName);
            } else {
                return new CommonQuery(queryName, queryFileName, paramFileName);
            }
        }
    }

    public static List<CommonQuery> initQueryList(Configuration configuration) throws Exception {
        String queryDir = configuration.getString(Configuration.QUERY_DIR);
        String parameterDir = configuration.getString(Configuration.QUERY_PARAMETER_DIR, null);
        List<CommonQuery> queryList = new ArrayList<>();
        String suffix = configuration.getString(Configuration.QUERY_FILE_SUFFIX);

        if (configuration.getBoolean(Configuration.QUERY_ALL_ENABLE, false)) {
            // Automatically add all queries in the specified directory
            File dir = new File(queryDir);
            if (dir.isDirectory()) {
                for (File file : dir.listFiles()) {
                    if (file.isFile() && file.getName().endsWith(suffix)) {
                        // assume the query name is the file name without the suffix
                        String queryName = file.getName().replace("." + suffix, "");
                        String queryFilePath = file.getAbsolutePath();
                        String parameterFilePath = null;
                        if (parameterDir != null && !parameterDir.isEmpty()) {
                            parameterFilePath =
                                    String.format("%s/%s.param", parameterDir, queryName);
                        }
                        queryList.add(
                                buildCommonQuery(queryName, queryFilePath, parameterFilePath));
                    }
                }
            } else {
                throw new IllegalArgumentException("queryDir is not a directory");
            }
            return queryList;
        }

        // for ldbc queries
        for (int index = 1; index <= 100; index++) {
            String enableQuery = String.format("ldbc_query_%d.enable", index);
            String queryFileName = String.format("ldbc_query_%d.%s", index, suffix);
            String parameterFileName = String.format("ldbc_query_%d.param", index);
            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("ldbc_query_%d", index);
                queryList.add(buildCommonQuery(queryName, queryFilePath, parameterFilePath));
            }
        }

        // for ldbc bi queries
        for (int index = 1; index <= 100; index++) {
            String enableQuery = String.format("bi_query_%d.enable", index);
            String queryFileName = String.format("bi_query_%d.%s", index, suffix);
            String parameterFileName = String.format("bi_query_%d.param", index);
            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("bi_query_%d", index);
                queryList.add(buildCommonQuery(queryName, queryFilePath, parameterFilePath));
            }
        }

        // for ldbc lsqb queries
        for (int index = 1; index <= 100; index++) {
            String enableQuery = String.format("lsqb_query_%d.enable", index);
            String queryFileName = String.format("lsqb_query_%d.%s", index, suffix);
            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String queryName = String.format("lsqb_query_%d", index);
                queryList.add(new QueryWithoutParameter(queryName, queryFilePath));
            }
        }

        // for k hop
        for (int index = 1; index < 5; index++) {
            String enableQuery = String.format("%d_hop_query.enable", index);
            String queryFileName = String.format("%d_hop_query.%s", index, suffix);
            String parameterFileName = String.format("%d_hop_query.param", index);

            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("%d_hop_query", index);
                queryList.add(new CommonQuery(queryName, queryFilePath, parameterFilePath));
            }
        }

        // for benchmarking early-stop queries
        String enableEsQuery = "early_stop_query.enable";
        String esQueryFileName = String.format("early_stop_query.%s", suffix);
        String esParameterFileName = "early_stop_query.param";
        if (configuration.getBoolean(enableEsQuery, false)) {
            String queryFilePath = String.format("%s/%s", queryDir, esQueryFileName);
            String parameterFilePath = String.format("%s/%s", parameterDir, esParameterFileName);
            String queryName = "early_stop_query";
            queryList.add(new CommonQuery(queryName, queryFilePath, parameterFilePath));
        }

        // for benchmarking subtask queries
        String enableSubtaskQuery = "subtask_query.enable";
        String subtaskQueryFileName = String.format("subtask_query.%s", suffix);
        String subtaskParameterFileName = "subtask_query.param";
        if (configuration.getBoolean(enableSubtaskQuery, false)) {
            String queryFilePath = String.format("%s/%s", queryDir, subtaskQueryFileName);
            String parameterFilePath =
                    String.format("%s/%s", parameterDir, subtaskParameterFileName);
            String queryName = "subtask_query";
            queryList.add(new SubtaskQuery(queryName, queryFilePath, parameterFilePath));
        }

        // custom queries without parameter
        for (int index = 1; index < 100; index++) {
            String enableQuery = String.format("custom_constant_query_%d.enable", index);
            String queryFileName = String.format("custom_constant_query_%d.%s", index, suffix);

            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String queryName = String.format("custom_constant_query_%d", index);

                queryList.add(new QueryWithoutParameter(queryName, queryFilePath));
            }
        }

        // custom queries
        for (int index = 1; index < 100; index++) {
            String enableQuery = String.format("custom_query_%d.enable", index);
            String queryFileName = String.format("custom_query_%d.%s", index, suffix);
            String parameterFileName = String.format("custom_query_%d.param", index);

            if (configuration.getBoolean(enableQuery, false)) {
                String queryFilePath = String.format("%s/%s", queryDir, queryFileName);
                String parameterFilePath = String.format("%s/%s", parameterDir, parameterFileName);
                String queryName = String.format("custom_query_%d", index);

                queryList.add(new CommonQuery(queryName, queryFilePath, parameterFilePath));
            }
        }

        return queryList;
    }
}
