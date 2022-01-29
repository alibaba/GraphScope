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
package com.alibaba.graphscope.gaia.common;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.util.HashMap;

public abstract class AbstractLdbcWithSubQuery extends CommonQuery {
    
    public AbstractLdbcWithSubQuery(String queryName, String queryFile, String parameterFile) throws Exception {
        super(queryName, queryFile, parameterFile);
    }

    @Override
    public void processGremlinQuery(Client client,
                                    HashMap<String, String> singleParameter,
                                    boolean printResult,
                                    boolean printQuery) {

        try {
            String gremlinQuery = generateGremlinQuery(singleParameter, queryPattern);

            long startTime = System.currentTimeMillis();
            ResultSet resultSet = client.submit(gremlinQuery);
            int resultCount = 0;
            String resultStr = "";

            for (Result result : resultSet) {
                String subQuery = buildSubQuery(result, singleParameter);
                ResultSet subQueryResult = client.submit(subQuery);
                Pair<Integer, String> resultPair = processResult(subQueryResult);
                resultCount += resultPair.getLeft();
                if (printResult && !resultPair.getRight().isEmpty()) {
                    resultStr = String.format("%s%s", resultStr, resultPair.getValue());
                }
            }
            long endTime = System.currentTimeMillis();
            long executeTime = endTime - startTime;
            if (printQuery) {
                String printInfo = String.format("QueryName[%s], Parameter[%s], ResultCount[%d], ExecuteTimeMS[%d].",
                        queryName,
                        singleParameter.toString(),
                        resultCount,
                        executeTime);
                if (printResult) {
                    printInfo = String.format("%s Result: { %s }",
                            printInfo,
                            resultStr);
                }
                System.out.println(printInfo);
            }
        }  catch (Exception e) {
            System.out.println(String.format("Timeout or failed: QueryName[%s], Parameter[%s].",
                    queryName,
                    singleParameter.toString()));
            e.printStackTrace();
        }
    }

    abstract String buildSubQuery(Result result, HashMap<String, String> singleParameter);
}
