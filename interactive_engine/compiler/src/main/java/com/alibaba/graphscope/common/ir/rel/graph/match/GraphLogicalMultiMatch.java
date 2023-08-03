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

package com.alibaba.graphscope.common.ir.rel.graph.match;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class GraphLogicalMultiMatch extends AbstractLogicalMatch {
    // sentences should >= 2
    private List<RelNode> sentences;

    protected GraphLogicalMultiMatch(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            @Nullable RelNode input,
            RelNode firstSentence,
            List<RelNode> otherSentences) {
        super(cluster, hints, input);
        ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
        this.sentences =
                builder.add(Objects.requireNonNull(firstSentence))
                        .addAll(ObjectUtils.requireNonEmpty(otherSentences))
                        .build();
    }

    public static GraphLogicalMultiMatch create(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            RelNode input,
            RelNode firstSentence,
            List<RelNode> otherSentences) {
        return new GraphLogicalMultiMatch(cluster, hints, input, firstSentence, otherSentences);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        Map<String, String> strMap = new LinkedHashMap<>();
        for (int i = 0; i < sentences.size(); ++i) {
            strMap.put(
                    String.format("s%d", i),
                    String.format("[%s]", RelOptUtil.toString(sentences.get(i))));
        }
        return super.explainTerms(pw).itemIf("sentences", strMap, !ObjectUtils.isEmpty(strMap));
    }

    @Override
    public RelDataType deriveRowType() {
//        List<RelNode> newSentences = Lists.newArrayList();
//        RelNode previous = null;
//        for (RelNode sentence : this.sentences) {
//            newSentences.add(reOrgAliasId(sentence, previous));
//            previous = sentence;
//        }
//        List<RelDataTypeField> fields = Lists.newArrayList();
//        for (RelNode sentence : newSentences) {
//            addFields(fields, sentence);
//        }
//        this.sentences = newSentences;
//        List<RelDataTypeField> dedup = fields.stream().distinct().collect(Collectors.toList());
//        return new RelRecordType(StructKind.FULLY_QUALIFIED, dedup);
        List<RelDataTypeField> fields = new ArrayList<>();
        for (RelNode node : sentences) {
            addFields(fields, node);
        }
        List<RelDataTypeField> dedup = fields.stream().distinct().collect(Collectors.toList());
        return new RelRecordType(StructKind.FULLY_QUALIFIED, dedup);
    }

    public List<RelNode> getSentences() {
        return Collections.unmodifiableList(sentences);
    }

//    private RelNode reOrgAliasId(RelNode topNode, RelNode previousSentence) {
//        List<RelNode> newInputs = topNode.getInputs().stream().map(k -> reOrgAliasId(k, previousSentence)).collect(Collectors.toList());
//        int newAliasId = ((GraphOptCluster) getCluster()).getIdGenerator().generate(getPreviousNodes(topNode, previousSentence), getAliasName(topNode));
//        RelNode newNode;
//        if (topNode instanceof GraphLogicalSource) {
//            GraphLogicalSource source = (GraphLogicalSource) topNode;
//            newNode = GraphLogicalSource.create(
//                    (GraphOptCluster) source.getCluster(),
//                    source.getHints(),
//                    source.getOpt(),
//                    source.getTableConfig(),
//                    source.getAliasName(),
//                    newAliasId);
//        } else if (topNode instanceof GraphLogicalExpand) {
//            GraphLogicalExpand expand = (GraphLogicalExpand) topNode;
//            newNode = GraphLogicalExpand.create(
//                    (GraphOptCluster) expand.getCluster(),
//                    expand.getHints(),
//                    newInputs.get(0),
//                    expand.getOpt(),
//                    expand.getTableConfig(),
//                    expand.getAliasName(),
//                    newAliasId);
//        } else if (topNode instanceof GraphLogicalGetV) {
//            GraphLogicalGetV getV = (GraphLogicalGetV) topNode;
//            newNode = GraphLogicalGetV.create(
//                    (GraphOptCluster) getV.getCluster(),
//                    getV.getHints(),
//                    newInputs.get(0),
//                    getV.getOpt(),
//                    getV.getTableConfig(),
//                    getV.getAliasName(),
//                    newAliasId);
//        } else if (topNode instanceof GraphLogicalPathExpand) {
//            GraphLogicalPathExpand pxd = (GraphLogicalPathExpand) topNode;
//            newNode = GraphLogicalPathExpand.create(
//                    (GraphOptCluster) pxd.getCluster(),
//                    ImmutableList.of(),
//                    newInputs.get(0),
//                    pxd.getExpand(),
//                    pxd.getGetV(),
//                    pxd.getOffset(),
//                    pxd.getFetch(),
//                    pxd.getResultOpt(),
//                    pxd.getPathOpt(),
//                    pxd.getAliasName(),
//                    newAliasId);
//        } else {
//            throw new UnsupportedOperationException("unsupported operator " + topNode.getClass() + " in match");
//        }
//        if ((topNode instanceof AbstractBindableTableScan)
//                && !ObjectUtils.isEmpty(((AbstractBindableTableScan) topNode).getFilters())) {
//            ((AbstractBindableTableScan) newNode).setFilters(((AbstractBindableTableScan) topNode).getFilters());
//        }
//        return newNode;
//    }
//
//    private String getAliasName(RelNode node) {
//        if (node instanceof AbstractBindableTableScan) {
//            return ((AbstractBindableTableScan) node).getAliasName();
//        } else if (node instanceof GraphLogicalPathExpand) {
//            return ((GraphLogicalPathExpand) node).getAliasName();
//        } else {
//            return null;
//        }
//    }
//
//    private List<RelNode> getPreviousNodes(RelNode curNode, RelNode previousSentence) {
//        List<RelNode> allPrevious = Lists.newArrayList();
//        if (!curNode.getInputs().isEmpty()) {
//            allPrevious.addAll(curNode.getInputs());
//        }
//        if (previousSentence != null) {
//            allPrevious.add(previousSentence);
//        }
//        return allPrevious;
//    }
}
