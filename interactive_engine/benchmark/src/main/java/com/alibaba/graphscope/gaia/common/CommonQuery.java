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

import com.alibaba.graphscope.gaia.clients.GraphClient;
import com.alibaba.graphscope.gaia.clients.GraphResultSet;
import com.alibaba.graphscope.gaia.utils.ResultComparator;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CommonQuery {
    String queryName;
    String queryPattern;
    private ArrayList<HashMap<String, String>> parameters;
    private static Logger logger = LoggerFactory.getLogger(CommonQuery.class);

    public CommonQuery(String queryName, String queryFile) throws Exception {
        this.queryName = queryName;
        this.queryPattern = getGraphQueryPattern(queryFile);
    }

    public CommonQuery(String queryName, String queryFile, String parameterFile) throws Exception {
        this.queryName = queryName;
        this.queryPattern = getGraphQueryPattern(queryFile);
        this.parameters = getParameters(parameterFile);
    }

    public String getQueryName() {
        return queryName;
    }

    public HashMap<String, String> getSingleParameter(int index) {
        return parameters.get(index % parameters.size());
    }

    public void processGraphQuery(
            GraphClient client,
            HashMap<String, String> singleParameter,
            boolean printResult,
            boolean printQuery,
            ResultComparator comparator) {
        try {
            String gremlinQuery = generateGraphQuery(singleParameter, queryPattern);
            long startTime = System.currentTimeMillis();
            GraphResultSet resultSet = client.submit(gremlinQuery);
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
                logger.info(printInfo);
            }
            if (!comparator.isEmpty()) {
                comparator.compareResults(queryName, result.getRight());
            }

        } catch (Exception e) {
            logger.error(
                    "Timeout or failed: QueryName[{}], Parameter[{}].", queryName, singleParameter);
            e.printStackTrace();
        }
    }

    String generateGraphQuery(HashMap<String, String> singleParameter, String gremlinQueryPattern) {
        for (String parameter : singleParameter.keySet()) {
            gremlinQueryPattern =
                    gremlinQueryPattern.replace(
                            getParameterPrefix() + parameter + getParameterPostfix(),
                            singleParameter.get(parameter));
        }
        return gremlinQueryPattern;
    }

    Pair<Integer, String> processResult(GraphResultSet resultSet) {
        int count = 0;
        String result = "";
        while (resultSet.hasNext()) {
            count += 1;
            result = String.format("%s\n%s", result, resultSet.next().toString());
        }
        return Pair.of(count, result);
    }

    private static String getGraphQueryPattern(String gremlinQueryPath) throws Exception {
        FileInputStream fileInputStream = new FileInputStream(gremlinQueryPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        String line;
        StringBuilder stringBuilder = new StringBuilder();
        while ((line = bufferedReader.readLine()) != null) {
            stringBuilder.append(line).append(" ");
        }
        return stringBuilder.toString().trim();
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

    // Support two formats of startDate: timestamp or "yyyyMMddHHmmssSSS"
    protected String getEndDate(String startDate, String durationDays) {
        boolean isTimeStamp = (startDate.matches("\\d+")) && (startDate.length() == 13);
        int days = Integer.parseInt(durationDays);
        try {
            if (isTimeStamp) {
                long startMillis = Long.parseLong(startDate);
                long endMillis = startMillis + TimeUnit.DAYS.toMillis(days);
                return Long.toString(endMillis);
            } else {
                // Assume the startDate is in the format of "yyyyMMddHHmmssSSS"
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                Date startDateObj = dateFormat.parse(startDate);
                long endMillis = startDateObj.getTime() + TimeUnit.DAYS.toMillis(days);
                Date endDateObj = new Date(endMillis);
                return dateFormat.format(endDateObj);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate end date", e);
        }
    }

    // Support two formats of startDate: timestamp or "yyyyMMddHHmmssSSS"
    protected String transformDate(String date) {
        // For standard date string, simply return it (if additional transformation is needed, add
        // here)
        return date;
    }

    protected String transformSimpleDate(String date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        TimeZone gmtTime = TimeZone.getTimeZone("UTC");
        format.setTimeZone(gmtTime);
        try {
            Date ori = format.parse(date);
            format.applyPattern("yyyyMMddHHmmssSSS");
            return format.format(ori);
        } catch (Exception e) {
            return format.format(new Date(Long.parseLong(date)));
        }
    }

    protected String transformDateTime(String date) {
        String ddate = date.split("\\.")[0].replace("T", " ");
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimeZone gmtTime = TimeZone.getTimeZone("UTC");
        format.setTimeZone(gmtTime);
        try {
            Date ori = format.parse(ddate);
            format.applyPattern("yyyyMMddHHmmssSSS");
            return format.format(ori);
        } catch (Exception e) {
            return format.format(new Date(Long.parseLong(date)));
        }
    }

    protected List<String> transformList(String list) {
        String[] transformList = list.split(";");
        List<String> transformArrayList = new ArrayList();
        for (String s : transformList) {
            String str = "\"" + s + "\"";
            transformArrayList.add(str);
        }
        return transformArrayList;
    }
}
