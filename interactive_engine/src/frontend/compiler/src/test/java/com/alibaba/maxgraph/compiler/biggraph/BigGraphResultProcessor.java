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
package com.alibaba.maxgraph.compiler.biggraph;

import com.alibaba.maxgraph.result.MapValueResult;
import com.alibaba.maxgraph.result.PropertyValueResult;
import com.alibaba.maxgraph.sdkcommon.graph.QueryResult;
import com.alibaba.maxgraph.result.VertexResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BigGraphResultProcessor {

    public static List<String> getExpectVertexList(String caseName) {
        try {
            List<String> resultList = IOUtils.readLines(Thread.currentThread().getContextClassLoader().getResourceAsStream("result/" + caseName), "utf-8");
            Collections.sort(resultList);
            return resultList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getResultVertexList(List<QueryResult> queryResultList) {
        List<String> resultList = Lists.newArrayList();
        for (QueryResult result : queryResultList) {
            resultList.add(String.valueOf(VertexResult.class.cast(result).id));
        }
        Collections.sort(resultList);

        return resultList;
    }

    public static List<Map<String, String>> getExpectMapValue(String caseName) {
        try {
            List<Map<String, String>> resultMapList = Lists.newArrayList();
            List<String> resultList = IOUtils.readLines(Thread.currentThread().getContextClassLoader().getResourceAsStream("result/" + caseName), "utf-8");
            for (String line : resultList) {
                Map<String, String> resultMap = Maps.newHashMap();
                String[] entryValueList = StringUtils.splitByWholeSeparator(line, ",");
                if (null != entryValueList && entryValueList.length > 0) {
                    for (String entryValue : entryValueList) {
                        String[] keyValueEntry = StringUtils.splitByWholeSeparator(entryValue, ":");
                        resultMap.put(keyValueEntry[0], keyValueEntry[1]);
                    }
                }
                resultMapList.add(resultMap);
            }

            return resultMapList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Map<String, String>> getResultMapList(List<QueryResult> resultList) {
        List<Map<String, String>> mapValueList = Lists.newArrayList();

        for (QueryResult result : resultList) {
            MapValueResult mapValueResult = MapValueResult.class.cast(result);
            Map<QueryResult, QueryResult> mapValue = mapValueResult.getValueMap();
            Map<String, String> resultMapValue = Maps.newHashMap();
            for (Map.Entry<QueryResult, QueryResult> entry : mapValue.entrySet()) {
                resultMapValue.put(
                        PropertyValueResult.class.cast(entry.getKey()).getValue().toString(),
                        PropertyValueResult.class.cast(entry.getValue()).getValue().toString());
            }
            mapValueList.add(resultMapValue);
        }

        return mapValueList;
    }
}
