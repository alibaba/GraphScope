package com.alibaba.graphscope.common.intermediate.calcite.type;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * a list of {@code IrSchemaType}, it is also a {@code IrSchemaType} which is a subclass of {@code RelDataType}.
 */
public class IrSchemaTypeList extends IrSchemaType implements List<IrSchemaType> {
    public IrSchemaTypeList(StructKind kind, List<RelDataTypeField> fields, boolean nullable) {
        super(kind, fields, nullable);
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
    public Iterator<IrSchemaType> iterator() {
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
    public boolean add(IrSchemaType irSchemaType) {
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
    public boolean addAll(Collection<? extends IrSchemaType> c) {
        return false;
    }

    @Override
    public boolean addAll(int index, Collection<? extends IrSchemaType> c) {
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
    public IrSchemaType get(int index) {
        return null;
    }

    @Override
    public IrSchemaType set(int index, IrSchemaType element) {
        return null;
    }

    @Override
    public void add(int index, IrSchemaType element) {}

    @Override
    public IrSchemaType remove(int index) {
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
    public ListIterator<IrSchemaType> listIterator() {
        return null;
    }

    @Override
    public ListIterator<IrSchemaType> listIterator(int index) {
        return null;
    }

    @Override
    public List<IrSchemaType> subList(int fromIndex, int toIndex) {
        return null;
    }
}
