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
package com.alibaba.maxgraph.common;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.HashMap;

public class HopQuery extends LdbcQuery {
    public HopQuery(String queryName, String queryFile, String parameterFile) throws Exception {
        super(queryName, queryFile, parameterFile);
    }

    @Override
    String generateGremlinQuery(
            HashMap<String, String> singleParameter, String gremlinQueryPattern) {

        for (String parameter : singleParameter.keySet()) {
            gremlinQueryPattern =
                    gremlinQueryPattern.replace(
                            getParameterPrefix() + parameter + getParameterPostfix(),
                            singleParameter.get(parameter));
        }
        return gremlinQueryPattern;
    }

    @Override
    public void processGremlinQuery(
            Client client,
            HashMap<String, String> singleParameter,
            boolean printResult,
            boolean printQuery) {

        String gremlinQuery = generateGremlinQuery(singleParameter, queryPattern);

        long startTime = System.currentTimeMillis();
        ResultSet resultSet = client.submit(gremlinQuery);
        Pair<Integer, String> result = processResult(resultSet);
        long endTime = System.currentTimeMillis();
        long executeTime = endTime - startTime;
        if (printQuery) {
            String printInfo =
                    String.format(
                            "QueryName[%s], Parameter[%s], ResultCount[%d], ExecuteTimeMS[%d].",
                            queryName, singleParameter.toString(), result.getLeft(), executeTime);
            if (printResult) {
                printInfo = String.format("%s Result: { %s }", printInfo, result.getRight());
            }
            System.out.println(printInfo);
        }
    }
}
