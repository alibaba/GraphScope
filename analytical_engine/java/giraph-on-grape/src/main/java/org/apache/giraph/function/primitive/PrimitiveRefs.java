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
package org.apache.giraph.function.primitive;

/**
 * Convenience classes holding primitive values - a reference
 * to a mutable value.
 * For use when lambdas need to mutate capturing primitive local variable.
 * (since lambdas cannot capture and modify local variables)
 */
public interface PrimitiveRefs {

  // Have public field for convenience, since classes have no logic inside
  // CHECKSTYLE: stop VisibilityModifierCheck

  /**
   * Convenience class holding int value,
   * for use when lambdas need to mutate capturing int local variable.
   */
  public class IntRef {
    /** value */
    public int value;

    /**
     * Constructor
     * @param value initial value
     */
    public IntRef(int value) {
      this.value = value;
    }
  }

  /**
   * Convenience class holding long value,
   * for use when lambdas need to mutate capturing long local variable.
   */
  public class LongRef {
    /** value */
    public long value;

    /**
     * Constructor
     * @param value initial value
     */
    public LongRef(long value) {
      this.value = value;
    }
  }

  /**
   * Convenience class holding int value,
   * for use when lambdas need to mutate capturing int local variable.
   */
  public class ShortRef {
    /** value */
    public short value;

    /**
     * Constructor
     * @param value initial value
     */
    public ShortRef(short value) {
      this.value = value;
    }
  }


  /**
   * Convenience class holding float value,
   * for use when lambdas need to mutate capturing float local variable.
   */
  public class FloatRef {
    /** value */
    public float value;

    /**
     * Constructor
     * @param value initial value
     */
    public FloatRef(float value) {
      this.value = value;
    }
  }

  /**
   * Convenience class holding double value,
   * for use when lambdas need to mutate capturing double local variable.
   */
  public class DoubleRef {
    /** value */
    public double value;

    /**
     * Constructor
     * @param value initial value
     */
    public DoubleRef(double value) {
      this.value = value;
    }
  }

  /**
   * Convenience class holding object values,
   * for use when lambdas need to mutate capturing object local variable.
   */
  public class ObjRef<O> {
    /** value */
    public O value;

    /**
     * Constructor
     * @param value initial value
     */
    public ObjRef(O value) {
      this.value = value;
    }
  }

  // CHECKSTYLE: resume VisibilityModifierCheck
}
