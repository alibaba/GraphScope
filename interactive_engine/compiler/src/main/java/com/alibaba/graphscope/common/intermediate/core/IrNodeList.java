package com.alibaba.graphscope.common.intermediate.core;

import org.apache.calcite.rel.type.RelDataType;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * maintain a list of {@code IrNode}.
 */
public class IrNodeList extends IrNode implements List<IrNode> {
    @Override
    public RelDataType inferReturnType() {
        return null;
    }

    @Override
    public void validate() {}

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
    public Iterator<IrNode> iterator() {
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
    public boolean add(IrNode irNode) {
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
    public boolean addAll(Collection<? extends IrNode> c) {
        return false;
    }

    @Override
    public boolean addAll(int index, Collection<? extends IrNode> c) {
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
    public IrNode get(int index) {
        return null;
    }

    @Override
    public IrNode set(int index, IrNode element) {
        return null;
    }

    @Override
    public void add(int index, IrNode element) {}

    @Override
    public IrNode remove(int index) {
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
    public ListIterator<IrNode> listIterator() {
        return null;
    }

    @Override
    public ListIterator<IrNode> listIterator(int index) {
        return null;
    }

    @Override
    public List<IrNode> subList(int fromIndex, int toIndex) {
        return null;
    }
}
