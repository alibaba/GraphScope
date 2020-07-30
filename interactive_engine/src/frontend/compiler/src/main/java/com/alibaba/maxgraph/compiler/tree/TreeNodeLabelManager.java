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
package com.alibaba.maxgraph.compiler.tree;

import com.alibaba.maxgraph.QueryFlowOuterClass;
import com.alibaba.maxgraph.compiler.tree.value.ValueType;
import com.alibaba.maxgraph.compiler.logical.LogicalVertex;
import com.alibaba.maxgraph.compiler.utils.CompilerUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.maxgraph.compiler.tree.TreeConstants.SYS_LABEL_START;
import static com.alibaba.maxgraph.compiler.tree.TreeConstants.USER_LABEL_START;
import static com.google.common.base.Preconditions.checkNotNull;

public class TreeNodeLabelManager implements Serializable {
    private int userLabelId;
    private int sysLabelId;
    private int createLabelIndex = 1;
    private Map<String, Integer> labelIndexList = Maps.newHashMap();
    private Map<Integer, String> indexLabelList = Maps.newHashMap();
    private transient Map<String, List<TreeNode>> labelTreeNodeList = Maps.newHashMap();
    private Map<Integer, String> vertexLabelList = Maps.newHashMap();
    private Map<Integer, String> vertexBeforeLabelList = Maps.newHashMap();

    private TreeNodeLabelManager() {
        this.userLabelId = USER_LABEL_START;
        this.sysLabelId = SYS_LABEL_START;

        labelIndexList.put(T.id.getAccessor(), TreeConstants.ID_INDEX);
        labelIndexList.put(T.label.getAccessor(), TreeConstants.LABEL_INDEX);
        labelIndexList.put(T.key.getAccessor(), TreeConstants.KEY_INDEX);
        labelIndexList.put(Column.keys.name(), TreeConstants.KEY_INDEX);
        labelIndexList.put(T.value.getAccessor(), TreeConstants.VALUE_INDEX);
        labelIndexList.put(Column.values.name(), TreeConstants.VALUE_INDEX);
        indexLabelList.put(TreeConstants.ID_INDEX, T.id.getAccessor());
        indexLabelList.put(TreeConstants.LABEL_INDEX, T.label.getAccessor());
        indexLabelList.put(TreeConstants.KEY_INDEX, T.key.getAccessor());
        indexLabelList.put(TreeConstants.VALUE_INDEX, T.value.getAccessor());
    }

    public static TreeNodeLabelManager createLabelManager() {
        return new TreeNodeLabelManager();
    }

    public void addUserTreeNodeLabel(String label, TreeNode treeNode) {
        if (labelIndexList.containsKey(label)) {
            labelTreeNodeList.get(label).add(treeNode);
        } else {
            int labelId = getUserLabelId();
            updateLabelIndexList(label, labelId);
            labelTreeNodeList.put(label, Lists.newArrayList(treeNode));
        }
        treeNode.addLabelStartRequirement(label);
    }

    private int getUserLabelId() {
        int labelId = userLabelId--;
        if (labelId <= SYS_LABEL_START) {
            throw new IllegalArgumentException("Too many label defined in the query");
        }
        return labelId;
    }

    public int getLabelIndex(String label) {
        if (labelIndexList.containsKey(label)) {
            return labelIndexList.get(label);
        } else {
            return TreeConstants.MAGIC_LABEL_ID;
        }
    }

    public TreeNode getFirstTreeNode(String label) {
        return labelTreeNodeList.get(label).get(0);
    }

    public TreeNode getLastTreeNode(String label) {
        List<TreeNode> treeNodeList = labelTreeNodeList.get(label);
        return treeNodeList.get(treeNodeList.size() - 1);
    }

    public List<TreeNode> getTreeNodeList(String label) {
        return checkNotNull(labelTreeNodeList.get(label));
    }

    public Set<String> getUserLabelList() {
        return labelIndexList
                .keySet()
                .stream()
                .filter(v -> labelIndexList.get(v) <= USER_LABEL_START && labelIndexList.get(v) > SYS_LABEL_START)
                .collect(Collectors.toSet());
    }

    public Map<String, Integer> getLabelIndexList() {
        return labelIndexList;
    }

    public String createSysLabelStart(LogicalVertex logicalVertex, String tag) {
        if (vertexLabelList.containsKey(logicalVertex.getId())) {
            return vertexLabelList.get(logicalVertex.getId());
        } else {
            String labelName = generateLabel(tag);
            int labelId = getSysLabelId();
            updateLabelIndexList(labelName, labelId);

            QueryFlowOuterClass.RequirementValue.Builder labelStartBuilder = null;
            for (QueryFlowOuterClass.RequirementValue.Builder builder : logicalVertex.getAfterRequirementList()) {
                if (builder.getReqType() == QueryFlowOuterClass.RequirementType.LABEL_START) {
                    labelStartBuilder = builder;
                    break;
                }
            }
            if (null == labelStartBuilder) {
                labelStartBuilder = QueryFlowOuterClass.RequirementValue.newBuilder()
                        .setReqType(QueryFlowOuterClass.RequirementType.LABEL_START);
                logicalVertex.getAfterRequirementList().add(labelStartBuilder);
            }
            labelStartBuilder.getReqArgumentBuilder().addIntValueList(labelId);

            vertexLabelList.put(logicalVertex.getId(), labelName);
            return labelName;
        }
    }

    public String createBeforeSysLabelStart(LogicalVertex logicalVertex, String tag) {
        if (vertexBeforeLabelList.containsKey(logicalVertex.getId())) {
            return vertexBeforeLabelList.get(logicalVertex.getId());
        } else {
            String labelName = generateLabel(tag);
            int labelId = getSysLabelId();
            updateLabelIndexList(labelName, labelId);

            QueryFlowOuterClass.RequirementValue.Builder labelStartBuilder = null;
            for (QueryFlowOuterClass.RequirementValue.Builder builder : logicalVertex.getBeforeRequirementList()) {
                if (builder.getReqType() == QueryFlowOuterClass.RequirementType.LABEL_START) {
                    labelStartBuilder = builder;
                    break;
                }
            }
            if (null == labelStartBuilder) {
                labelStartBuilder = QueryFlowOuterClass.RequirementValue.newBuilder()
                        .setReqType(QueryFlowOuterClass.RequirementType.LABEL_START);
                logicalVertex.getBeforeRequirementList().add(labelStartBuilder);
            }
            labelStartBuilder.getReqArgumentBuilder().addIntValueList(labelId);

            vertexBeforeLabelList.put(logicalVertex.getId(), labelName);
            return labelName;
        }
    }

    public boolean containsVertexLabel(LogicalVertex logicalVertex) {
        return vertexLabelList.containsKey(logicalVertex.getId());
    }

    public String createSysLabelStart(String tag) {
        String labelName = generateLabel(tag);
        int labelId = getSysLabelId();
        updateLabelIndexList(labelName, labelId);
        return labelName;
    }

    private int getSysLabelId() {
        return sysLabelId--;
    }

    private void updateLabelIndexList(String labelName, int labelId) {
        labelIndexList.put(labelName, labelId);
        indexLabelList.put(labelId, labelName);
    }

    private String generateLabel(String tag) {
        return "mx_" + tag + "_" + createLabelIndex++;
    }

    public ValueType getValueType(String label, Pop pop) {
        return CompilerUtils.parseValueTypeWithPop(labelTreeNodeList.get(label), pop);
    }

    public List<TreeNode> getLabelTreeNodeList(String label) {
        return labelTreeNodeList.get(label);
    }

    public String getLabelName(int labelId) {
        return indexLabelList.get(labelId);
    }

    public Map<Integer, String> getUserIndexLabelList() {
        Map<Integer, String> userIndexLabelList = Maps.newHashMap();
        for (Map.Entry<Integer, String> entry : indexLabelList.entrySet()) {
            if (entry.getKey() <= TreeConstants.USER_LABEL_START && entry.getKey() > TreeConstants.SYS_LABEL_START) {
                userIndexLabelList.put(entry.getKey(), entry.getValue());
            }
        }

        return userIndexLabelList;
    }

    public void replaceLabelName(String label, String targetName) {
        int labelId = labelIndexList.remove(label);
        labelIndexList.put(targetName, labelId);
        indexLabelList.put(labelId, targetName);
    }
}
