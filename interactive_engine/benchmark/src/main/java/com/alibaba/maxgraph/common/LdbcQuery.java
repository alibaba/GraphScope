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
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

public class LdbcQuery {
    String queryName;
    String queryPattern;
    private ArrayList<HashMap<String, String>> parameters;

    public LdbcQuery(String queryName, String queryFile) throws Exception {
        this.queryName = queryName;
        this.queryPattern = getGremlinQueryPattern(queryFile);
    }

    public LdbcQuery(String queryName, String queryFile, String parameterFile) throws Exception {
        this.queryName = queryName;
        this.queryPattern = getGremlinQueryPattern(queryFile);
        this.parameters = getParameters(parameterFile);
    }

    public HashMap<String, String> getSingleParameter(int index) {
        return parameters.get(index % parameters.size());
    }

    public void processGremlinQuery(
            Client client,
            HashMap<String, String> singleParameter,
            boolean printResult,
            boolean printQuery) {
        try {
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
                                queryName,
                                singleParameter.toString(),
                                result.getLeft(),
                                executeTime);
                if (printResult) {
                    printInfo = String.format("%s Result: { %s }", printInfo, result.getRight());
                }
                System.out.println(printInfo);
            }

        } catch (Exception e) {
            System.out.println(
                    String.format(
                            "Timeout or failed: QueryName[%s], Parameter[%s].",
                            queryName, singleParameter.toString()));
            e.printStackTrace();
        }
    }

    String generateGremlinQuery(
            HashMap<String, String> singleParameter, String gremlinQueryPattern) {
        String[] subStrSet = gremlinQueryPattern.split("\\$");

        if (subStrSet.length != singleParameter.size() + 1) {
            String errMsg =
                    "Illegal parameter and query, caused by the number of parameter is not suitable"
                            + " of query";
            System.out.println(errMsg);
            throw new RuntimeException(
                    "Illegal parameter and query, caused by the number of parameter is not suitable"
                            + " of query");
        }

        String gremlinQuery = subStrSet[0];

        for (int i = 1; i < subStrSet.length; i++) {
            String subStr = subStrSet[i];
            for (int j = 0; j < subStr.length(); j++) {
                if (!Character.isLetterOrDigit(subStr.charAt(j))) {
                    gremlinQuery =
                            String.format(
                                    "%s%s%s",
                                    gremlinQuery, singleParameter.get(i - 1), subStr.substring(j));
                    break;
                }
            }
        }

        return gremlinQuery;
    }

    Pair<Integer, String> processResult(ResultSet resultSet) {
        Iterator<Result> iterator = resultSet.iterator();
        int count = 0;
        String result = "";
        while (iterator.hasNext()) {
            count += 1;
            result = String.format("%s\n%s", result, iterator.next().toString());
        }
        return Pair.of(count, result);
    }

    private static String getGremlinQueryPattern(String gremlinQueryPath) throws Exception {
        FileInputStream fileInputStream = new FileInputStream(gremlinQueryPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        return bufferedReader.readLine();
    }

    private static ArrayList<HashMap<String, String>> getParameters(String parameterFilePath)
            throws Exception {
        ArrayList<HashMap<String, String>> parameters = new ArrayList<>();
        FileInputStream fileInputStream = new FileInputStream(parameterFilePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        String headerStr = bufferedReader.readLine();
        String[] headerSet = headerStr.split("\\|");
        String idStr;
        while ((idStr = bufferedReader.readLine()) != null) {
            String[] idSet = idStr.split("\\|");
            if (headerSet.length != idSet.length) {
                throw new RuntimeException("there is parameter is empty");
            }
            HashMap<String, String> parameter = new HashMap<>();
            for (int i = 0; i < headerSet.length; i++) {
                parameter.put(headerSet[i], idSet[i]);
            }
            parameters.add(parameter);
        }
        bufferedReader.close();
        fileInputStream.close();

        return parameters;
    }

    protected String getParameterPrefix() {
        return "$";
    }

    protected String getParameterPostfix() {
        return "";
    }

    protected String getEndDate(String startDate, String durationDays) {
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS"); // date format
        try {
            Date sDate = format.parse(startDate);
            sDate.after(new Date(Long.parseLong(durationDays) * 24 * 3600 * 1000));
            return format.format(
                    new Date(sDate.getTime() + (Long.parseLong(durationDays) * 24 * 3600 * 1000)));
        } catch (Exception e) {
            return String.valueOf(
                    Long.parseLong(startDate) + (Long.parseLong(durationDays) * 24 * 3600 * 1000));
        }
    }
}
