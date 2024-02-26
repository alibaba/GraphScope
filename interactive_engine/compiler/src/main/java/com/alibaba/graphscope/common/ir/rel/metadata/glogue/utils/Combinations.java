/*
 * Copyright 2024 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rel.metadata.glogue.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Combinations {
    public static <T> List<List<T>> getCombinations(List<T> elements, int k) {
        List<List<T>> results = new ArrayList<>();
        getCombinationsHelper(elements, k, 0, new ArrayList<>(), results);
        return results;
    }

    private static <T> void getCombinationsHelper(
            List<T> elements,
            int k,
            int startIndex,
            List<T> currentCombination,
            List<List<T>> results) {
        if (currentCombination.size() == k) {
            results.add(new ArrayList<>(currentCombination));
            return;
        }
        if (startIndex >= elements.size()
                || elements.size() - startIndex < k - currentCombination.size()) {
            return;
        }
        // notice that we do not allow duplicates within the combination
        // if (currentCombination.contains(elements.get(startIndex) )) return;
        currentCombination.add(elements.get(startIndex));
        getCombinationsHelper(elements, k, startIndex + 1, currentCombination, results);
        currentCombination.remove(currentCombination.size() - 1);
        getCombinationsHelper(elements, k, startIndex + 1, currentCombination, results);
    }

    public static void main(String[] args) {
        List<Integer> elements = Arrays.asList(1, 2, 3, 3);
        for (int k = 1; k <= elements.size(); k++) {
            List<List<Integer>> combinations = Combinations.getCombinations(elements, k);
            for (List<Integer> comb : combinations) {
                System.out.println(comb);
            }
        }
    }
}
