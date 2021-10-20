/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.cost;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** part of every row */
public class CostRow {

    private CostMappingManager costMappingManager;
    private Set<String> birthFieldList = Sets.newHashSet();
    private Set<RowField> fieldList = Sets.newHashSet();

    public CostRow(List<RowField> fieldList) {
        this(fieldList, false);
    }

    public CostRow(List<RowField> fieldList, boolean birthFlag) {
        this.fieldList.addAll(fieldList);
        if (birthFlag) {
            for (RowField rowField : fieldList) {
                birthFieldList.addAll(rowField.getFieldList());
            }
        }
    }

    public Set<String> getBirthFieldList() {
        return this.birthFieldList;
    }

    public void mergeCostRow(CostRow costRow) {
        Set<RowField> resultRowFieldList = Sets.newHashSet();
        if (fieldList.isEmpty()) {
            resultRowFieldList = Sets.newHashSet(costRow.getFieldList());
        } else {
            for (RowField rowField : fieldList) {
                if (costRow.getFieldList().isEmpty()) {
                    Set<String> fieldValueList = Sets.newHashSet(rowField.getFieldList());
                    resultRowFieldList.add(new RowField(fieldValueList));
                } else {
                    for (RowField otherRowField : costRow.getFieldList()) {
                        Set<String> fieldValueList = Sets.newHashSet(rowField.getFieldList());
                        fieldValueList.addAll(otherRowField.getFieldList());
                        Set<String> fieldParentList =
                                fieldValueList.stream()
                                        .filter(
                                                v ->
                                                        fieldValueList.contains(
                                                                CostUtils.buildValueName(v)))
                                        .collect(Collectors.toSet());
                        for (String fieldParent : fieldParentList) {
                            fieldValueList.remove(CostUtils.buildValueName(fieldParent));
                        }
                        resultRowFieldList.add(new RowField(fieldValueList));
                    }
                }
            }
        }
        this.fieldList = resultRowFieldList;
        this.getBirthFieldList().addAll(costRow.getBirthFieldList());
    }

    public Set<RowField> getFieldList() {
        return this.fieldList;
    }
}
