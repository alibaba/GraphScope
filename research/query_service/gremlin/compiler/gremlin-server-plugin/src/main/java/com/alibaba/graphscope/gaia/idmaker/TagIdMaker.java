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
package com.alibaba.graphscope.gaia.idmaker;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

// per query
public class TagIdMaker implements IdMaker<Integer, String> {
    private static final Logger logger = LoggerFactory.getLogger(TagIdMaker.class);
    private Map<String, Integer> tagIds = new HashMap<>();
    // one byte
    public static final int VALID_BITS = 0x0ff;
    public static String HIDDEN_TAG_PREFIX = "~HIDDEN_TAG_";
    private AtomicInteger hiddenCounter;
    private AtomicInteger tagCounter;

    public TagIdMaker(Traversal.Admin admin) {
        tagCounter = new AtomicInteger(0);
        hiddenCounter = new AtomicInteger(getTags(admin).size());
    }

    @Override
    public Integer getId(String tagName) {
        if (tagIds.get(tagName) == null) {
            int tagId;
            if (isHiddenTag(tagName)) {
                tagId = hiddenCounter.getAndIncrement() & VALID_BITS;
            } else {
                tagId = tagCounter.getAndIncrement() & VALID_BITS;
            }
            tagIds.put(tagName, tagId);
        }
        return tagIds.get(tagName);
    }

    public static boolean isHiddenTag(String tagName) {
        return tagName.startsWith(HIDDEN_TAG_PREFIX);
    }

    private Set<String> getTags(Traversal.Admin admin) {
        Set<String> tags = new HashSet<>();
        List<Step> steps = admin.getSteps();
        for (int i = 0; i < steps.size(); ++i) {
            Step step = steps.get(i);
            tags.addAll((Set) step.getLabels().stream().filter(k -> !isHiddenTag((String) k)).collect(Collectors.toSet()));
            if (step instanceof TraversalParent) {
                Iterator var1 = ((TraversalParent) step).getLocalChildren().iterator();
                while (var1.hasNext()) {
                    tags.addAll(getTags((Traversal.Admin) var1.next()));
                }
                Iterator var2 = ((TraversalParent) step).getGlobalChildren().iterator();
                while (var2.hasNext()) {
                    tags.addAll(getTags((Traversal.Admin) var2.next()));
                }
            }
        }
        return tags;
    }

    public String getTagById(int id) {
        for (Map.Entry<String, Integer> entry : tagIds.entrySet()) {
            if (entry.getValue() == id) {
                return entry.getKey();
            }
        }
        logger.error("id {} not exist, return empty string", id);
        return "";
    }
}
