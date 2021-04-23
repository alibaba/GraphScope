package com.alibaba.maxgraph.v2.frontend.compiler.cost;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * part of every row
 */
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
                        Set<String> fieldParentList = fieldValueList.stream()
                                .filter(v -> fieldValueList.contains(CostUtils.buildValueName(v)))
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
