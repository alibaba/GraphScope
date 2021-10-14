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
package com.alibaba.maxgraph.compiler;

import com.google.common.collect.Lists;

import java.util.List;

public class TestMain {
    public static void main(String[] args) {
        List<List<String>> matrix = Lists.newArrayList(
                Lists.newArrayList("v11", "v12"),
                Lists.newArrayList("v21", "v22", "v23"),
                Lists.newArrayList(),
                Lists.newArrayList("v31", "v32", "v33", "v34"),
                Lists.newArrayList("v41"));

        List<List<String>> resultList = Lists.newArrayList();
        traversal(matrix, Lists.newArrayList(0, 0, 0, 0, 0), resultList);

        System.out.println(resultList);
    }

    private static void traversal(List<List<String>> matrix, List<Integer> indexList, List<List<String>> resultList) {
        List<String> result = Lists.newArrayList();
        for (int i = 0; i < indexList.size(); i++) {
            int index = indexList.get(i);
            if (matrix.get(i).size() > 0) {
                result.add(matrix.get(i).get(index));
            }
        }
        resultList.add(result);
        if (nextIndexList(matrix, indexList, indexList.size() - 1)) {
            traversal(matrix, indexList, resultList);
        }
    }

    private static boolean nextIndexList(List<List<String>> matrix, List<Integer> indexList, int index) {
        int currIndex = indexList.get(index);
        int currCount = matrix.get(index).size();
        if (index == 0 && currIndex == currCount - 1) {
            return false;
        }

        if (currIndex == currCount - 1 || currCount == 0) {
            indexList.set(index, 0);
            return nextIndexList(matrix, indexList, index - 1);
        }

        indexList.set(index, currIndex + 1);
        return true;
    }
}
