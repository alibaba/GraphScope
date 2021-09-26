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
package org.apache.giraph.function;

import org.apache.giraph.function.vertex.ConsumerWithVertex;
import org.apache.giraph.function.vertex.SupplierFromVertex;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Default object transfer, intermediary between producers and consumers.
 *
 * Holds value temporarily in memory, so can be used only when producer and
 * consumer are in the same context. Nulls it out after supplying, so each
 * object is returned only once, and second consecutive call to 'get' will
 * return null.
 *
 * Useful for both:
 *
 * - passing data from vertexReceive function of WorkerReceivePiece of previous
 * Piece to vertexSend function WorkerSendPiece of next Piece, of the same
 * vertex.
 * - when value is set on the master, and later read in block logic
 * (RepeatUntilBlock), or in a different Piece, either on worker or master.
 * If it is read within the same piece - just use local field.
 *
 * @param <T> Type of object to transfer.
 */
public class ObjectTransfer<T> implements Supplier<T>, Consumer<T> {
  /** value */
  private T value;

  /**
   * Constructor
   * @param value initial value
   */
  public ObjectTransfer(T value) {
    this.value = value;
  }

  /** Constructor */
  public ObjectTransfer() {
  }

  @Override
  public T get() {
    T result = value;
    value = null;
    return result;
  }

  @Override
  public void apply(T value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return getClass() + " [value=" + value + "]";
  }

  /**
   * To be called when needing to pass it as a Supplier - making it
   * obvious that K, V and E on supplier side can be any types,
   * and to make code work without compile errors/warnings.
   *
   * In Java7, some callsites might need explicit types:
   * object.&lt;LongWritable, DoubleWritable, Writable&gt;castToSupplier()
   * In Java8, object.castToSupplier() is always going to be enough.
   *
   * @param <I> Vertex id type
   * @param <V> Vertex value type
   * @param <E> Edge value type
   * @return supplier from vertex
   */
  // TODO Java8: cleanup callers
  @SuppressWarnings("rawtypes")
  public <I extends WritableComparable, V extends Writable, E extends Writable>
  SupplierFromVertex<I, V, E, T> castToSupplier() {
    return new SupplierFromVertex<I, V, E, T>() {
      @Override
      public T get(Vertex<I, V, E> vertex) {
        return ObjectTransfer.this.get();
      }
    };
  }

  /**
   * To be called when needing to pass it as a Consumer - making it
   * obvious that K, V and E on consumer side can be any types,
   * and to make code work without compile errors/warnings.
   *
   * In Java7, some callsites might need explicit types:
   * object.&lt;LongWritable, DoubleWritable, Writable&gt;castToConsumer()
   * In Java8, object.castToConsumer() is always going to be enough.
   *
   * @param <I> Vertex id type
   * @param <V> Vertex value type
   * @param <E> Edge value type
   * @return consumer with vertex
   */
  // TODO Java8: cleanup callers
  @SuppressWarnings("rawtypes")
  public <I extends WritableComparable, V extends Writable, E extends Writable>
  ConsumerWithVertex<I, V, E, T> castToConsumer() {
    return new ConsumerWithVertex<I, V, E, T>() {
      @Override
      public void apply(Vertex<I, V, E> vertex, T value) {
        ObjectTransfer.this.apply(value);
      }
    };
  }
}
