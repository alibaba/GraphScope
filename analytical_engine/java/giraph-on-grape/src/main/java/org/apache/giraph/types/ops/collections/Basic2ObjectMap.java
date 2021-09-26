/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.types.ops.collections;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.giraph.types.ops.IntTypeOps;
import org.apache.giraph.types.ops.LongTypeOps;
import org.apache.giraph.types.ops.PrimitiveIdTypeOps;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Basic2ObjectMap with only basic set of operations.
 * All operations that return object K are returning reusable object,
 * which is modified after calling any other function.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public abstract class Basic2ObjectMap<K, V> implements Writable {
  /** Removes all of the elements from this list. */
  public abstract void clear();
  /**
   * Number of elements in this list
   * @return size
   */
  public abstract int size();

  /**
   * Checks whether key is present in the map
   * @param key Key
   * @return true if present
   */
  public abstract boolean containsKey(K key);
  /**
   * Adds a pair to the map.
   *
   * @param key Key
   * @param value Value.
   * @return the old value, or null if no value was present for the given key.
   */
  public abstract V put(K key, V value);
  /**
   * Get value for a given key
   * @param key Key
   * @return Value, or null
   */
  public abstract V get(K key);
  /**
   * Removes the mapping with the given key.
   *
   * @param key Key
   * @return the old value, or null if no value was present for the given key.
   */
  public abstract V remove(K key);
  /**
   * TypeOps for type of keys this object holds
   * @return TypeOps
   */
  public abstract PrimitiveIdTypeOps<K> getKeyTypeOps();
  /**
   * Fast iterator over keys within this map, which doesn't allocate new
   * element for each returned element.
   *
   * Object returned by next() is only valid until next() is called again,
   * because it is reused.
   *
   * @return Iterator
   */
  public abstract Iterator<K> fastKeyIterator();

  /**
   * Iterator over map values.
   *
   * @return Iterator
   */
  public abstract Iterator<V> valueIterator();

  /**
   * A collection of all values.
   *
   * @return Iterator
   */
  public abstract Collection<V> values();

  /**
   * Iterator that reuses key object.
   *
   * @param <Iter> Primitive key iterator type
   */
  protected abstract class ReusableIterator<Iter extends Iterator<?>>
      implements ResettableIterator<K> {
    /** Primitive Key iterator */
    protected Iter iter;
    /** Reusable key object */
    protected final K reusableKey = getKeyTypeOps().create();

    /**
     * Constructor
     * @param iter Primitive Key iterator
     */
    public ReusableIterator(Iter iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public void remove() {
      iter.remove();
    }
  }

  /** IntWritable implementation of Basic2ObjectMap */
  public static final class BasicInt2ObjectOpenHashMap<V>
      extends Basic2ObjectMap<IntWritable, V> {
    /** Map */
    private final Int2ObjectOpenHashMap<V> map;
    /** Value writer */
    private final WritableWriter<V> valueWriter;

    /**
     * Constructor
     *
     * @param valueWriter Writer of values
     */
    public BasicInt2ObjectOpenHashMap(WritableWriter<V> valueWriter) {
      this.map = new Int2ObjectOpenHashMap<>();
      this.valueWriter = valueWriter;
    }

    /**
     * Constructor
     *
     * @param capacity Capacity
     * @param valueWriter Writer of values
     */
    public BasicInt2ObjectOpenHashMap(
        int capacity, WritableWriter<V> valueWriter) {
      this.map = new Int2ObjectOpenHashMap<>(capacity);
      this.valueWriter = valueWriter;
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public boolean containsKey(IntWritable key) {
      return map.containsKey(key.get());
    }

    @Override
    public V put(IntWritable key, V value) {
      return map.put(key.get(), value);
    }

    @Override
    public V get(IntWritable key) {
      return map.get(key.get());
    }

    @Override
    public V remove(IntWritable key) {
      return map.remove(key.get());
    }

    @Override
    public PrimitiveIdTypeOps<IntWritable> getKeyTypeOps() {
      return IntTypeOps.INSTANCE;
    }

    @Override
    public Iterator<IntWritable> fastKeyIterator() {
      return new ReusableIterator<IntIterator>(map.keySet().iterator()) {
        @Override
        public IntWritable next() {
          reusableKey.set(iter.nextInt());
          return reusableKey;
        }

        @Override
        public void reset() {
          iter = map.keySet().iterator();
        }
      };
    }

    @Override
    public Iterator<V> valueIterator() {
      return map.values().iterator();
    }

    @Override
    public Collection<V> values() {
      return map.values();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(map.size());
      ObjectIterator<Int2ObjectMap.Entry<V>> iterator =
          map.int2ObjectEntrySet().fastIterator();
      while (iterator.hasNext()) {
        Int2ObjectMap.Entry<V> entry = iterator.next();
        out.writeInt(entry.getIntKey());
        valueWriter.write(out, entry.getValue());
      }
    }

    @Override
    public void readFields(DataInput in)
      throws IOException {
      int size = in.readInt();
      map.clear();
      map.trim(size);
      while (size-- > 0) {
        int key = in.readInt();
        V value = valueWriter.readFields(in);
        map.put(key, value);
      }
    }
  }

  /** LongWritable implementation of Basic2ObjectMap */
  public static final class BasicLong2ObjectOpenHashMap<V>
      extends Basic2ObjectMap<LongWritable, V> {
    /** Map */
    private final Long2ObjectOpenHashMap<V> map;
    /** Value writer */
    private final WritableWriter<V> valueWriter;

    /**
     * Constructor
     *
     * @param valueWriter Writer of values
     */
    public BasicLong2ObjectOpenHashMap(WritableWriter<V> valueWriter) {
      this.map = new Long2ObjectOpenHashMap<>();
      this.valueWriter = valueWriter;
    }

    /**
     * Constructor
     *
     * @param capacity Capacity
     * @param valueWriter Writer of values
     */
    public BasicLong2ObjectOpenHashMap(
        int capacity, WritableWriter<V> valueWriter) {
      this.map = new Long2ObjectOpenHashMap<>(capacity);
      this.valueWriter = valueWriter;
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public boolean containsKey(LongWritable key) {
      return map.containsKey(key.get());
    }

    @Override
    public V put(LongWritable key, V value) {
      return map.put(key.get(), value);
    }

    @Override
    public V get(LongWritable key) {
      return map.get(key.get());
    }

    @Override
    public V remove(LongWritable key) {
      return map.remove(key.get());
    }

    @Override
    public PrimitiveIdTypeOps<LongWritable> getKeyTypeOps() {
      return LongTypeOps.INSTANCE;
    }

    @Override
    public Iterator<LongWritable> fastKeyIterator() {
      return new ReusableIterator<LongIterator>(map.keySet().iterator()) {
        @Override
        public LongWritable next() {
          reusableKey.set(iter.nextLong());
          return reusableKey;
        }

        @Override
        public void reset() {
          iter = map.keySet().iterator();
        }
      };
    }

    @Override
    public Iterator<V> valueIterator() {
      return map.values().iterator();
    }

    @Override
    public Collection<V> values() {
      return map.values();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Preconditions.checkState(
        valueWriter != null,
        "valueWriter is not provided"
      );

      out.writeInt(map.size());
      ObjectIterator<Long2ObjectMap.Entry<V>> iterator =
          map.long2ObjectEntrySet().fastIterator();
      while (iterator.hasNext()) {
        Long2ObjectMap.Entry<V> entry = iterator.next();
        out.writeLong(entry.getLongKey());
        valueWriter.write(out, entry.getValue());
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      Preconditions.checkState(
        valueWriter != null,
        "valueWriter is not provided"
      );

      int size = in.readInt();
      map.clear();
      map.trim(size);
      while (size-- > 0) {
        long key = in.readLong();
        V value = valueWriter.readFields(in);
        map.put(key, value);
      }
    }
  }

  /** Writable implementation of Basic2ObjectMap */
  public static final class BasicObject2ObjectOpenHashMap<K extends Writable, V>
      extends Basic2ObjectMap<K, V> {
    /** Map */
    private final Object2ObjectOpenHashMap<K, V> map;
    /** Key writer */
    private final WritableWriter<K> keyWriter;
    /** Value writer */
    private final WritableWriter<V> valueWriter;

    /**
     * Constructor
     *
     * @param keyWriter Writer of keys
     * @param valueWriter Writer of values
     */
    public BasicObject2ObjectOpenHashMap(
      WritableWriter<K> keyWriter,
      WritableWriter<V> valueWriter
    ) {
      this.map = new Object2ObjectOpenHashMap<>();
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    /**
     * Constructor
     *
     * @param capacity Map capacity
     * @param keyWriter Writer of keys
     * @param valueWriter Writer of values
     */
    public BasicObject2ObjectOpenHashMap(
      int capacity,
      WritableWriter<K> keyWriter,
      WritableWriter<V> valueWriter
    ) {
      this.map = new Object2ObjectOpenHashMap<>(capacity);
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public int size() {
      return map.size();
    }

    @Override
    public boolean containsKey(K key) {
      return map.containsKey(key);
    }

    @Override
    public V put(K key, V value) {
      // we need a copy since the key object is mutable
      K copyKey = WritableUtils.createCopy(key);
      return map.put(copyKey, value);
    }

    @Override
    public V get(K key) {
      return map.get(key);
    }

    @Override
    public V remove(K key) {
      return map.remove(key);
    }

    @Override
    public PrimitiveIdTypeOps<K> getKeyTypeOps() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<K> fastKeyIterator() {
      return map.keySet().iterator();
    }

    @Override
    public Iterator<V> valueIterator() {
      return map.values().iterator();
    }

    @Override
    public Collection<V> values() {
      return map.values();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Preconditions.checkState(
        keyWriter != null,
        "keyWriter is not provided"
      );
      Preconditions.checkState(
        valueWriter != null,
        "valueWriter is not provided"
      );

      out.writeInt(map.size());
      ObjectIterator<Object2ObjectMap.Entry<K, V>> iterator =
          map.object2ObjectEntrySet().fastIterator();
      while (iterator.hasNext()) {
        Object2ObjectMap.Entry<K, V> entry = iterator.next();
        keyWriter.write(out, entry.getKey());
        valueWriter.write(out, entry.getValue());
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      Preconditions.checkState(
        keyWriter != null,
        "keyWriter is not provided"
      );
      Preconditions.checkState(
        valueWriter != null,
        "valueWriter is not provided"
      );

      int size = in.readInt();
      map.clear();
      map.trim(size);
      while (size-- > 0) {
        K key = keyWriter.readFields(in);
        V value = valueWriter.readFields(in);
        map.put(key, value);
      }
    }
  }
}
