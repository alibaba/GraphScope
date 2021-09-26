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

package org.apache.giraph.utils;

import com.alibaba.graphscope.utils.ExtendedByteArrayDataInput;
import com.alibaba.graphscope.utils.ExtendedByteArrayDataOutput;
import com.alibaba.graphscope.utils.UnsafeByteArrayInputStream;
import com.alibaba.graphscope.utils.UnsafeByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.ValueFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;


/**
 * Helper static methods for working with Writable objects.
 */
public class WritableUtils {

    /**
     * Don't construct.
     */
    private WritableUtils() {
    }

    /**
     * Instantiate a new Writable, checking for NullWritable along the way.
     *
     * @param klass Class
     * @param <W>   type
     * @return new instance of class
     */
    public static <W extends Writable> W createWritable(Class<W> klass) {
        return createWritable(klass, null);
    }

    /**
     * Instantiate a new Writable, checking for NullWritable along the way.
     *
     * @param klass         Class
     * @param configuration Configuration
     * @param <W>           type
     * @return new instance of class
     */
    public static <W extends Writable> W createWritable(
        Class<W> klass,
        ImmutableClassesGiraphConfiguration configuration) {
        W result;
        if (NullWritable.class.equals(klass)) {
            result = (W) NullWritable.get();
        } else {
            result = ReflectionUtils.newInstance(klass);
        }
        ConfigurationUtils.configureIfPossible(result, configuration);
        return result;
    }

    /**
     * Read class from data input. Matches {@link #writeClass(Class, DataOutput)}.
     *
     * @param input Data input
     * @param <T>   Class type
     * @return Class, or null if null was written
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> readClass(DataInput input) throws IOException {
        if (input.readBoolean()) {
            String className = input.readUTF();
            try {
                return (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("readClass: No class found " +
                    className);
            }
        } else {
            return null;
        }
    }

    /**
     * Write class to data output. Also handles the case when class is null.
     *
     * @param clazz  Class
     * @param output Data output
     * @param <T>    Class type
     */
    public static <T> void writeClass(Class<T> clazz,
        DataOutput output) throws IOException {
        output.writeBoolean(clazz != null);
        if (clazz != null) {
            output.writeUTF(clazz.getName());
        }
    }

    /**
     * Write object to output stream
     * @param object Object
     * @param output Output stream
     * @throws IOException
     */
    public static void writeWritableObject(
        Writable object, DataOutput output)
        throws IOException {
        output.writeBoolean(object != null);
        if (object != null) {
            output.writeUTF(object.getClass().getName());
            object.write(output);
        }
    }

    /**
     * Reads object from input stream
     * @param input Input stream
     * @param conf Configuration
     * @param <T> Object type
     * @return Object
     * @throws IOException
     */
    public static <T extends Writable>
    T readWritableObject(DataInput input,
        ImmutableClassesGiraphConfiguration conf) throws IOException {
        if (input.readBoolean()) {
            String className = input.readUTF();
            try {
                T object =
                    (T) ReflectionUtils.newInstance(Class.forName(className), conf);
                object.readFields(input);
                return object;
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("readWritableObject: No class found " +
                    className);
            }
        } else {
            return null;
        }
    }
    /**
     * Writes enum into a stream, by serializing class name and it's index
     * @param enumValue Enum value
     * @param output Output stream
     * @param <T> Enum type
     */
    public static <T extends Enum<T>> void writeEnum(T enumValue,
        DataOutput output) throws IOException {
        writeClass(
            enumValue != null ? enumValue.getDeclaringClass() : null, output);
        if (enumValue != null) {
            Varint.writeUnsignedVarInt(enumValue.ordinal(), output);
        }
    }

    /**
     * Reads enum from the stream, serialized by writeEnum
     * @param input Input stream
     * @param <T> Enum type
     * @return Enum value
     */
    public static <T extends Enum<T>> T readEnum(DataInput input) throws
        IOException {
        Class<T> clazz = readClass(input);
        if (clazz != null) {
            int ordinal = Varint.readUnsignedVarInt(input);
            try {
                T[] values = (T[]) clazz.getDeclaredMethod("values").invoke(null);
                return values[ordinal];
            } catch (IllegalAccessException | IllegalArgumentException |
                InvocationTargetException | NoSuchMethodException |
                SecurityException e) {
                throw new IOException("Cannot read enum", e);
            }
        } else {
            return null;
        }
    }


    /**
     * Create a copy of Writable object, by serializing and deserializing it.
     *
     * @param reusableOut Reusable output stream to serialize into
     * @param reusableIn Reusable input stream to deserialize out of
     * @param original Original value of which to make a copy
     * @param conf Configuration
     * @param <T> Type of the object
     * @return Copy of the original value
     */
    public static <T extends Writable> T createCopy(
        UnsafeByteArrayOutputStream reusableOut,
        UnsafeReusableByteArrayInput reusableIn, T original,
        ImmutableClassesGiraphConfiguration conf) {
        T copy = (T) createWritable(original.getClass(), conf);

        try {
            reusableOut.reset();
            original.write(reusableOut);
            reusableIn.initialize(
                reusableOut.getByteArray(), 0, reusableOut.getPos());
            copy.readFields(reusableIn);

            if (reusableIn.available() != 0) {
                throw new RuntimeException("Serialization of " +
                    original.getClass() + " encountered issues, " +
                    reusableIn.available() + " bytes left to be read");
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                "IOException occurred while trying to create a copy " +
                    original.getClass(), e);
        }
        return copy;
    }

    /**
     * Create a copy of Writable object, by serializing and deserializing it.
     *
     * @param original Original value of which to make a copy
     * @return Copy of the original value
     * @param <T> Type of the object
     */
    public static final <T extends Writable> T createCopy(T original) {
        return (T) createCopy(original, original.getClass(), null);
    }

    /**
     * Create a copy of Writable object, by serializing and deserializing it.
     *
     * @param original Original value of which to make a copy
     * @param outputClass Expected copy class, needs to match original
     * @param conf Configuration
     * @return Copy of the original value
     * @param <T> Type of the object
     */
    public static final <T extends Writable>
    T createCopy(T original, Class<? extends T> outputClass,
        ImmutableClassesGiraphConfiguration conf) {
        T result = WritableUtils.createWritable(outputClass, conf);
        copyInto(original, result);
        return result;
    }
    /**
     * Create a copy of Writable object, by serializing and deserializing it.
     *
     * @param original Original value of which to make a copy
     * @param classFactory Factory to create new empty object from
     * @param conf Configuration
     * @return Copy of the original value
     * @param <T> Type of the object
     */
    public static final <T extends Writable>
    T createCopy(T original, ValueFactory<T> classFactory,
        ImmutableClassesGiraphConfiguration conf) {
        T result = classFactory.newInstance();
        copyInto(original, result);
        return result;
    }

    /**
     * Copy {@code from} into {@code to}, by serializing and deserializing it.
     * Since it is creating streams inside, it's mostly useful for
     * tests/non-performant code.
     *
     * @param from Object to copy from
     * @param to Object to copy into
     * @param <T> Type of the object
     */
    public static <T extends Writable> void copyInto(T from, T to) {
        copyInto(from, to, false);
    }

    /**
     * Copy {@code from} into {@code to}, by serializing and deserializing it.
     * Since it is creating streams inside, it's mostly useful for
     * tests/non-performant code.
     *
     * @param from Object to copy from
     * @param to Object to copy into
     * @param checkOverRead if true, will add one more byte at the end of writing,
     *                      to make sure read is not touching it. Useful for tests
     * @param <T> Type of the object
     */
    public static <T extends Writable> void copyInto(
        T from, T to, boolean checkOverRead) {
        try {
            if (from.getClass() != to.getClass()) {
                throw new RuntimeException(
                    "Trying to copy from " + from.getClass() +
                        " into " + to.getClass());
            }

            UnsafeByteArrayOutputStream out = new UnsafeByteArrayOutputStream();
            from.write(out);
            if (checkOverRead) {
                out.writeByte(0);
            }

            UnsafeByteArrayInputStream in =
                new UnsafeByteArrayInputStream(out.getByteArray(), 0, out.getPos());
            to.readFields(in);

            if (in.available() != (checkOverRead ? 1 : 0)) {
                throw new RuntimeException(
                    "Serialization encountered issues with " + from.getClass() + ", " +
                        (in.available() - (checkOverRead ? 1 : 0)) + " fewer bytes read");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Deserialize from given byte array into given writable,
     * using new instance of ExtendedByteArrayDataInput.
     *
     * @param data Byte array representing writable
     * @param to Object to fill
     * @param <T> Type of the object
     */
    public static <T extends Writable> void fromByteArray(byte[] data, T to) {
        try {
            ExtendedByteArrayDataInput in =
                new ExtendedByteArrayDataInput(data, 0, data.length);
            to.readFields(in);

            if (in.available() != 0) {
                throw new RuntimeException(
                    "Serialization encountered issues, " + in.available() +
                        " bytes left to be read");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serialize given writable to byte array,
     * using new instance of ExtendedByteArrayDataOutput.
     *
     * @param w Writable object
     * @return array of bytes
     * @param <T> Type of the object
     */
    public static <T extends Writable> byte[] toByteArray(T w) {
        try {
            ExtendedByteArrayDataOutput out = new ExtendedByteArrayDataOutput();
            w.write(out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}