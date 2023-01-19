package com.alibaba.graphscope.common.calcite.schema.type;

import com.alibaba.graphscope.common.calcite.rel.builder.config.ScanOpt;

import org.apache.commons.lang3.NotImplementedException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * a list of {@code IrSchemaType}, to denote fuzzy conditions in a vertex or an edge, i.e. g.V() or g.V().hasLabel("person", "software")
 */
public class GraphSchemaTypeList extends GraphSchemaType implements List<GraphSchemaType> {
    public GraphSchemaTypeList(ScanOpt opt, List<GraphSchemaType> list) {
        super(opt);
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
