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

package com.alibaba.graphscope.common.ir.type;

import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A list of {@code IrSchemaType}, to denote fuzzy conditions in a vertex or an edge, i.e. g.V() or g.V().hasLabel("person", "software")
 */
public class GraphSchemaTypeList extends GraphSchemaType implements List<GraphSchemaType> {
    private List<GraphSchemaType> schemaTypes;

    protected GraphSchemaTypeList(GraphOpt.Source scanOpt, List<GraphSchemaType> schemaTypes, List<RelDataTypeField> fields) {
        super(scanOpt, fields);
        this.schemaTypes = schemaTypes;
    }

    public static GraphSchemaTypeList create(List<GraphSchemaType> list) {
        ObjectUtils.requireNonEmpty(list);
        GraphOpt.Source scanOpt = list.get(0).getScanOpt();
        List<String> labelOpts = new ArrayList<>();
        List<RelDataTypeField> fields = new ArrayList<>();
        for (GraphSchemaType type : list) {
            labelOpts.add("{label=" + type.labelType.getLabel() + ", opt=" + type.scanOpt + "}");
            if (type.getScanOpt() != scanOpt) {
                throw new IllegalArgumentException(
                        "fuzzy label types should have the same opt, but is " + labelOpts);
            }
            fields.addAll(type.getFieldList());
        }
        return new GraphSchemaTypeList(scanOpt, list, fields.stream().distinct().collect(Collectors.toList()));
    }

    @Override
    public int size() {
        return this.schemaTypes.size();
    }

    @Override
    public boolean isEmpty() {
        return this.schemaTypes.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.schemaTypes.contains(o);
    }

    @Override
    public Iterator<GraphSchemaType> iterator() {
        return this.schemaTypes.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.schemaTypes.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return this.schemaTypes.toArray(a);
    }

    @Override
    public boolean add(GraphSchemaType graphSchemaType) {
        return this.schemaTypes.add(graphSchemaType);
    }

    @Override
    public boolean remove(Object o) {
        return this.schemaTypes.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.schemaTypes.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends GraphSchemaType> c) {
        return this.schemaTypes.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends GraphSchemaType> c) {
        return this.schemaTypes.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.schemaTypes.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.schemaTypes.retainAll(c);
    }

    @Override
    public void clear() {
        this.schemaTypes.clear();
    }

    @Override
    public GraphSchemaType get(int index) {
        return this.schemaTypes.get(index);
    }

    @Override
    public GraphSchemaType set(int index, GraphSchemaType element) {
        return this.schemaTypes.set(index, element);
    }

    @Override
    public void add(int index, GraphSchemaType element) {
        this.schemaTypes.add(index, element);
    }

    @Override
    public GraphSchemaType remove(int index) {
        return this.schemaTypes.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return this.schemaTypes.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return this.schemaTypes.lastIndexOf(o);
    }

    @Override
    public ListIterator<GraphSchemaType> listIterator() {
        return this.schemaTypes.listIterator();
    }

    @Override
    public ListIterator<GraphSchemaType> listIterator(int index) {
        return this.schemaTypes.listIterator(index);
    }

    @Override
    public List<GraphSchemaType> subList(int fromIndex, int toIndex) {
        return this.schemaTypes.subList(fromIndex, toIndex);
    }
}
