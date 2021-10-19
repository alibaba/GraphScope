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
package com.alibaba.maxgraph.sdkcommon.exception;

import com.alibaba.maxgraph.proto.StorageEngine;
import com.alibaba.maxgraph.sdkcommon.meta.DataType;
import com.alibaba.maxgraph.sdkcommon.util.ExceptionUtils.ErrorCode;

import java.text.MessageFormat;
import java.util.List;

public class MetaException extends MaxGraphException {
    public MetaException(ErrorCode code, String msg) {
        super(code, msg);
    }

    public MetaException(String msg) {
        this(ErrorCode.Unknown, msg);
    }

    public static MetaException typeAlreadyExist(String label) {
        String msg = MessageFormat.format("type: {0} is already exist", label);
        return new MetaException(ErrorCode.TypeAlreadyExist, msg);
    }

    public static MetaException typePropertyAlreadyExist(String label, int propertyName) {
        String msg = MessageFormat.format("Property Id: {0} of {1} is already exists.", propertyName, label);
        return new MetaException(ErrorCode.PropertyAlreadyExist, msg);
    }

    public static MetaException propertyAlreadyExist(String propertyName) {
        String msg = MessageFormat.format("PropertyName: {0} is already exists.", propertyName);
        return new MetaException(ErrorCode.PropertyAlreadyExist, msg);
    }

    public static MetaException propertyNotExist(String propertyName) {
        return new MetaException(ErrorCode.PropertyNotExist, "Property: " + propertyName + " is undefined. ");
    }

    public static MetaException propertyExistInType(String propertyName, List<String> labels) {
        return new MetaException(ErrorCode.PropertyExistInType, "Property: " + propertyName + " can not be removed, " +
                "because it exists in these types: " + labels);
    }

    public static MetaException typeNotFound(String label) {
        String msg = MessageFormat.format("Type: {0} not found.", label);
        return new MetaException(ErrorCode.TypeNotFound, msg);
    }

    public static MetaException typeNotFound(int id) {
        String msg = MessageFormat.format("Type with id :{0} not found.", id);
        return new MetaException(ErrorCode.TypeNotFound, msg);
    }

    public static RuntimeException asRuntimeException(ErrorCode code, String msg) {
        throw new RuntimeException(new MetaException(code, msg));
    }

    public static MetaException relationShipAlreadyExist(String srcLabel, String edgeLabel, String dstLabel) {
        String msg = MessageFormat.format("relationship: with srcLabel :{0}, edgeLabel: {1}, dstLabel: {2} is already exist", srcLabel, edgeLabel, dstLabel);
        return new MetaException(ErrorCode.RelationShipAlreadyExist, msg);
    }

    public static MetaException relationShipNotExist(String srcLabel, String edgeLabel, String dstLabel) {
        String msg = MessageFormat.format("relationship: with srcLabel :{0}, edgeLabel: {1}, dstLabel: {2} is not " +
                "exist", srcLabel, edgeLabel, dstLabel);
        return new MetaException(ErrorCode.RelationShipNotExist, msg);
    }

    public static MetaException dropTypeException(String label) {
        String msg = MessageFormat.format("drop type: {0} exception ,because of exist relationship", label);
        return new MetaException(ErrorCode.RelationShipExistWithType, msg);
    }

    public static MetaException indexAlreadyExist(String name) {
        String msg = MessageFormat.format("Index is Already exist: {0}", name);
        return new MetaException(msg);
    }

    public static MetaException indexNotExist(String name) {
        String msg = MessageFormat.format("Index is not exist: {0}", name);
        return new MetaException(msg);
    }

    public static MetaException indexShouldNotUsedOnVertex(String indexName) {
        String msg = MessageFormat.format("Index can be used only on edge type: {0}", indexName);
        return new MetaException(ErrorCode.IndexCanBeUsedOnlyOnEdge, msg);
    }

    public static MaxGraphException indexTypeMustBeUnique(String name) {
        String msg = MessageFormat.format("Index Type Must be unique in one Vertex/Edge: {0}", name);
        return new MetaException(ErrorCode.IndexTypeMustUnique, msg);
    }

    public static MaxGraphException vertexPrimaryChanged() {
        return new MetaException(ErrorCode.InvalidTypeChanged, "Can not change the primary key of Vertex" +
                " Type");
    }

    public static MaxGraphException storageEngineChanged(StorageEngine from, StorageEngine to) {
        return new MetaException(ErrorCode.InvalidTypeChanged, "Can not change Storage Engine from " + from
                .name() + " to " + to.name());
    }

    public static MetaException propertyTypeChanged(String propertyName) {
        return new MetaException(ErrorCode.InvalidTypeChanged, "Property: " + propertyName + " type can not " +
                "be changed");
    }

    public static MaxGraphException dimensionTypeChanged(boolean from, boolean to) {
        return new MetaException(ErrorCode.InvalidTypeChanged, "Can not change dimensionType from " + wrapDimension
                (from) + " to " + wrapDimension(to));
    }

    public static MaxGraphException typeChanged() {
        return new MetaException(ErrorCode.InvalidTypeChanged, "Can not change Vertex Or Edge's type and label");
    }

    public static MaxGraphException dataTypeNotValid(DataType dataType) {
        return new MetaException(ErrorCode.DataTypeNotValid, "unSupported data type: " + dataType.toString());
    }

    private static String wrapDimension(boolean isDimension) {
        return isDimension ? "dimension" : "not dimension";
    }
}
