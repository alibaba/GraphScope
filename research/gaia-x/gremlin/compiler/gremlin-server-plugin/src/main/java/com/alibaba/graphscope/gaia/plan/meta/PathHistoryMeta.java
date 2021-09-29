/*
 * Copyright 2020 Alibaba Group Holding Limited.
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
package com.alibaba.graphscope.gaia.plan.meta;

import com.alibaba.graphscope.gaia.plan.meta.object.TraverserElement;

import java.util.*;
import java.util.stream.Collectors;

public class PathHistoryMeta implements Meta<String, TraverserElement> {
    private List<TraverserElement> path = new ArrayList<>();
    private Map<String, Integer> tagIdxMap = new HashMap<>();

    @Override
    public void add(String tag, TraverserElement data) {
        int occ = path.indexOf(data);
        if (occ == -1) {
            path.add(data);
            occ = path.size() - 1;
        }
        tagIdxMap.put(tag, occ);
    }

    @Override
    public void delete(String tag) {
        if (tagIdxMap.get(tag) != null) {
            path.remove(tagIdxMap.get(tag));
            tagIdxMap.remove(tag);
        }
    }

    @Override
    public Optional<TraverserElement> get(String tag) {
        if (tagIdxMap.get(tag) == null) {
            return Optional.ofNullable(null);
        } else {
            return Optional.of(path.get(tagIdxMap.get(tag)));
        }
    }

    @Override
    public List<String> getAllObjects() {
        return tagIdxMap.keySet().stream().collect(Collectors.toList());
    }

    public PathHistoryMeta fork() {
        PathHistoryMeta newOne = new PathHistoryMeta();
        for (String tag : getAllObjects()) {
            newOne.add(tag, get(tag).get().fork());
        }
        return newOne;
    }
}
