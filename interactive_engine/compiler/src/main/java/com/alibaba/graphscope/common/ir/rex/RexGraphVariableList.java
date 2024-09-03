/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.rex;

import com.alibaba.graphscope.common.ir.tools.AliasInference;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// denote a list of RexGraphVariable, to handle the special case of gremlin `head` if the input
// operator outputs multiple columns
public class RexGraphVariableList extends RexGraphVariable implements List<RexGraphVariable> {
    private final List<RexGraphVariable> innerList;

    protected RexGraphVariableList(
            int aliasId,
            int columnId,
            @Nullable String name,
            RelDataType type,
            List<RexGraphVariable> innerList) {
        super(aliasId, columnId, name, type);
        this.innerList = ObjectUtils.requireNonEmpty(innerList);
    }

    public static RexGraphVariable of(List<RexGraphVariable> innerList) {
        if (innerList.size() == 1) return innerList.get(0);
        List<String> digest =
                innerList.stream().map(var -> var.getName()).collect(Collectors.toList());
        RelRecordType fullyType =
                new RelRecordType(
                        StructKind.FULLY_QUALIFIED,
                        innerList.stream()
                                .map(
                                        var -> {
                                            String[] splits =
                                                    var.getName()
                                                            .split(
                                                                    Pattern.quote(
                                                                            AliasInference
                                                                                    .DELIMITER));
                                            String aliasName =
                                                    splits.length > 0
                                                            ? splits[0]
                                                            : AliasInference.DEFAULT_NAME;
                                            return new RelDataTypeFieldImpl(
                                                    aliasName, var.getAliasId(), var.getType());
                                        })
                                .collect(Collectors.toList()));
        return new RexGraphVariableList(
                AliasInference.DEFAULT_ID,
                AliasInference.DEFAULT_COLUMN_ID,
                digest.toString(),
                fullyType,
                innerList);
    }

    @Override
    public int size() {
        return innerList.size();
    }

    @Override
    public boolean isEmpty() {
        return innerList.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return innerList.contains(o);
    }

    @Override
    public Iterator<RexGraphVariable> iterator() {
        return innerList.iterator();
    }

    @Override
    public Object[] toArray() {
        return innerList.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return innerList.toArray(a);
    }

    @Override
    public boolean add(RexGraphVariable variable) {
        return innerList.add(variable);
    }

    @Override
    public boolean remove(Object o) {
        return innerList.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return innerList.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends RexGraphVariable> c) {
        return innerList.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends RexGraphVariable> c) {
        return innerList.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return innerList.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return innerList.retainAll(c);
    }

    @Override
    public void clear() {
        innerList.clear();
    }

    @Override
    public RexGraphVariable get(int index) {
        return innerList.get(index);
    }

    @Override
    public RexGraphVariable set(int index, RexGraphVariable element) {
        return innerList.set(index, element);
    }

    @Override
    public void add(int index, RexGraphVariable element) {
        innerList.add(index, element);
    }

    @Override
    public RexGraphVariable remove(int index) {
        return innerList.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return innerList.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return innerList.lastIndexOf(o);
    }

    @Override
    public ListIterator<RexGraphVariable> listIterator() {
        return innerList.listIterator();
    }

    @Override
    public ListIterator<RexGraphVariable> listIterator(int index) {
        return innerList.listIterator(index);
    }

    @Override
    public List<RexGraphVariable> subList(int fromIndex, int toIndex) {
        return innerList.subList(fromIndex, toIndex);
    }
}
