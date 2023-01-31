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

package com.alibaba.graphscope.common.calcite.type;

import com.alibaba.graphscope.common.calcite.tools.config.ScanOpt;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A list of {@code IrSchemaType}, to denote fuzzy conditions in a vertex or an edge, i.e. g.V() or g.V().hasLabel("person", "software")
 */
public class GraphSchemaTypeList extends GraphSchemaType implements List<GraphSchemaType> {
    protected GraphSchemaTypeList(ScanOpt scanOpt, List<RelDataTypeField> fields) {
        super(scanOpt, fields);
    }

    public static GraphSchemaTypeList create(List<GraphSchemaType> list) {
        ObjectUtils.requireNonEmpty(list);
        ScanOpt scanOpt = list.get(0).getScanOpt();
        List<RelDataTypeField> fields = new ArrayList<>();
        List<String> labelOpts = new ArrayList<>();
        for (GraphSchemaType type : list) {
            labelOpts.add("{label=" + type.labelType.getLabel() + ", opt=" + type.scanOpt + "}");
            if (type.getScanOpt() != scanOpt) {
                throw new IllegalArgumentException(
                        "fuzzy label types should have the same opt, but is " + labelOpts);
            }
            fields.addAll(type.getFieldList());
        }
        return new GraphSchemaTypeList(
                scanOpt, fields.stream().distinct().collect(Collectors.toList()));
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return false;
    }

    @Override
    public Iterator<GraphSchemaType> iterator() {
        return null;
    }

    @Override
    public Object[] toArray() {
        return new Object[0];
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;
    }

    @Override
    public boolean add(GraphSchemaType irSchemaType) {
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends GraphSchemaType> c) {
        return false;
    }

    @Override
    public boolean addAll(int index, Collection<? extends GraphSchemaType> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {}

    @Override
    public GraphSchemaType get(int index) {
        return null;
    }

    @Override
    public GraphSchemaType set(int index, GraphSchemaType element) {
        return null;
    }

    @Override
    public void add(int index, GraphSchemaType element) {}

    @Override
    public GraphSchemaType remove(int index) {
        return null;
    }

    @Override
    public int indexOf(Object o) {
        return 0;
    }

    @Override
    public int lastIndexOf(Object o) {
        return 0;
    }

    @Override
    public ListIterator<GraphSchemaType> listIterator() {
        return null;
    }

    @Override
    public ListIterator<GraphSchemaType> listIterator(int index) {
        return null;
    }

    @Override
    public List<GraphSchemaType> subList(int fromIndex, int toIndex) {
        return null;
    }

    @Override
    public LabelType getLabelType() {
        throw new NotImplementedException("");
    }

    public List<LabelType> getLabelTypes() {
        return null;
    }
}
