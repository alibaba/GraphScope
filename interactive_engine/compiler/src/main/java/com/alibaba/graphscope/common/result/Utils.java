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

package com.alibaba.graphscope.common.result;

import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.gaia.proto.Common;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static String getLabelName(
            Common.NameOrId nameOrId, @Nullable GraphLabelType labelTypes) {
        switch (nameOrId.getItemCase()) {
            case NAME:
                return nameOrId.getName();
            case ID:
            default:
                List<Integer> labelIds = new ArrayList<>();
                if (labelTypes != null) {
                    for (GraphLabelType.Entry labelType : labelTypes.getLabelsEntry()) {
                        if (labelType.getLabelId() == nameOrId.getId()) {
                            return labelType.getLabel();
                        }
                        labelIds.add(labelType.getLabelId());
                    }
                }
                logger.warn(
                        "label id={} not found, expected ids are {}", nameOrId.getId(), labelIds);
                return String.valueOf(nameOrId.getId());
        }
    }

    public static String getSrcLabelName(
            Common.NameOrId nameOrId, @Nullable GraphLabelType labelTypes) {
        switch (nameOrId.getItemCase()) {
            case NAME:
                return nameOrId.getName();
            case ID:
            default:
                List<Integer> labelIds = new ArrayList<>();
                if (labelTypes != null) {
                    for (GraphLabelType.Entry labelType : labelTypes.getLabelsEntry()) {
                        if (labelType.getSrcLabelId() == nameOrId.getId()) {
                            return labelType.getSrcLabel();
                        }
                    }
                }
                logger.warn(
                        "src label id={} not found, expected ids are {}",
                        nameOrId.getId(),
                        labelIds);
                return String.valueOf(nameOrId.getId());
        }
    }

    public static String getDstLabelName(
            Common.NameOrId nameOrId, @Nullable GraphLabelType labelTypes) {
        switch (nameOrId.getItemCase()) {
            case NAME:
                return nameOrId.getName();
            case ID:
            default:
                List<Integer> labelIds = new ArrayList<>();
                if (labelTypes != null) {
                    for (GraphLabelType.Entry labelType : labelTypes.getLabelsEntry()) {
                        if (labelType.getDstLabelId() == nameOrId.getId()) {
                            return labelType.getDstLabel();
                        }
                    }
                }
                logger.warn(
                        "dst label id={} not found, expected ids are {}",
                        nameOrId.getId(),
                        labelIds);
                return String.valueOf(nameOrId.getId());
        }
    }

    public static @Nullable GraphLabelType getLabelTypes(RelDataType dataType) {
        if (dataType instanceof GraphSchemaType) {
            return ((GraphSchemaType) dataType).getLabelType();
        } else {
            return null;
        }
    }

    public static RelDataType getVertexType(RelDataType graphPathType) {
        return (graphPathType instanceof GraphPathType)
                ? ((GraphPathType) graphPathType).getComponentType().getGetVType()
                : graphPathType;
    }

    public static RelDataType getEdgeType(RelDataType graphPathType) {
        return (graphPathType instanceof GraphPathType)
                ? ((GraphPathType) graphPathType).getComponentType().getExpandType()
                : graphPathType;
    }

    public static String parseLabelValue(Common.Value value, GraphLabelType type) {
        switch (value.getItemCase()) {
            case STR:
                return value.getStr();
            case I32:
                return parseLabelValue(value.getI32(), type);
            case I64:
                return parseLabelValue(value.getI64(), type);
            default:
                throw new IllegalArgumentException(
                        "cannot parse label value with type=" + value.getItemCase().name());
        }
    }

    public static String parseLabelValue(long labelId, GraphLabelType type) {
        List<Object> expectedLabelIds = Lists.newArrayList();
        for (GraphLabelType.Entry entry : type.getLabelsEntry()) {
            if (entry.getLabelId() == labelId) {
                return entry.getLabel();
            }
            expectedLabelIds.add(entry.getLabelId());
        }
        throw new IllegalArgumentException(
                "cannot parse label value="
                        + labelId
                        + " from expected type="
                        + type
                        + ", expected ids are "
                        + expectedLabelIds);
    }

    public static RelDataTypeField findFieldByPredicate(
            Predicate<RelDataTypeField> p, List<RelDataTypeField> typeFields) {
        return typeFields.stream().filter(k -> p.test(k)).findFirst().orElse(null);
    }
}
