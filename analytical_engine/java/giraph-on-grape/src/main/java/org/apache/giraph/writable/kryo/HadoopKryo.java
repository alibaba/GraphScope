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
package org.apache.giraph.writable.kryo;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ReferenceResolver;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.SerializerFactory;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.esotericsoftware.kryo.util.ObjectMap;
import com.google.common.base.Preconditions;

import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.types.ops.collections.Basic2ObjectMap;
import org.apache.giraph.types.ops.collections.BasicSet;
import org.apache.giraph.writable.kryo.markers.KryoIgnoreWritable;
import org.apache.giraph.writable.kryo.markers.NonKryoWritable;
import org.apache.giraph.writable.kryo.serializers.ArraysAsListSerializer;
import org.apache.giraph.writable.kryo.serializers.CollectionsNCopiesSerializer;
import org.apache.giraph.writable.kryo.serializers.DirectWritableSerializer;
import org.apache.giraph.writable.kryo.serializers.FastUtilSerializer;
import org.apache.giraph.writable.kryo.serializers.ImmutableBiMapSerializerUtils;
import org.apache.giraph.writable.kryo.serializers.ReusableFieldSerializer;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

/**
 * Kryo instance that provides serialization through DataInput/DataOutput that
 * org.apache.hadoop.io.Writable uses.
 * <p>
 * All public APIs are static.
 * <p>
 * It extends Kryo to reuse KryoPool functionality, but have additional needed objects cached as
 * well. If we move to ThreadLocal or other caching technique, we can use composition, instead of
 * inheritance here.
 */
public class HadoopKryo extends Kryo {

    /**
     * List of interfaces/parent classes that will not be allowed to be serialized, together with
     * explanation of why, that will be shown when throwing such exception
     */
    private static final Map<Class<?>, String> NON_SERIALIZABLE;
    /**
     * Pool of reusable Kryo objects, since they are expensive to create
     */
    private static final KryoPool KRYO_POOL =
            new KryoPool.Builder(
                            new KryoFactory() {
                                @Override
                                public Kryo create() {
                                    return createKryo(true, true);
                                }
                            })
                    .build();
    /**
     * Thread local HadoopKryo object
     */
    private static final ThreadLocal<HadoopKryo> KRYO =
            new ThreadLocal<HadoopKryo>() {
                @Override
                protected HadoopKryo initialValue() {
                    return createKryo(false, false);
                }
            };

    static {
        NON_SERIALIZABLE = new LinkedHashMap<>();
        NON_SERIALIZABLE.put(
                NonKryoWritable.class,
                "it is marked to not allow serialization, " + "look at the class for more details");
        NON_SERIALIZABLE.put(KryoWritableWrapper.class, "recursion is disallowed");
        NON_SERIALIZABLE.put(
                Configuration.class, "it cannot be supported since it contains ClassLoader");
        NON_SERIALIZABLE.put(GiraphConfigurationSettable.class, "configuration cannot be set");
        NON_SERIALIZABLE.put(Configurable.class, "configuration cannot be set");
        NON_SERIALIZABLE.put(
                Random.class,
                "it should be rarely serialized, since it would create same stream "
                        + "of numbers everywhere, use TransientRandom instead");
        NON_SERIALIZABLE.put(Logger.class, "Logger must be a static field");
    }

    /**
     * Map of already initialized serializers used for readIntoObject/writeOutOfObject pair of
     * methods
     */
    private final ObjectMap<Class<?>, ReusableFieldSerializer<Object>> classToIntoSerializer =
            new ObjectMap<>();
    /**
     * Reusable Input object
     */
    private InputChunked input;
    /**
     * Reusable Output object
     */
    private OutputChunked output;
    /**
     * Reusable DataInput wrapper stream
     */
    private DataInputWrapperStream dataInputWrapperStream;
    /**
     * Reusable DataOutput wrapper stream
     */
    private DataOutputWrapperStream dataOutputWrapperStream;

    /**
     * Hide constructor, so all access go through pool of cached objects
     */
    private HadoopKryo() {}

    /**
     * Constructor that takes custom class resolver and reference resolver.
     *
     * @param classResolver     Class resolver
     * @param referenceResolver Reference resolver
     */
    private HadoopKryo(ClassResolver classResolver, ReferenceResolver referenceResolver) {
        super(classResolver, referenceResolver);
    }

    // Public API:

    /**
     * Write type of given object and the object itself to the output stream. Inverse of
     * readClassAndObj.
     *
     * @param out    Output stream
     * @param object Object to write
     */
    public static void writeClassAndObj(final DataOutput out, final Object object) {
        writeInternal(out, object, false);
    }

    /**
     * Read object from the input stream, by reading first type of the object, and then all of its
     * fields. Inverse of writeClassAndObject.
     *
     * @param in  Input stream
     * @param <T> Type of the object being read
     * @return Deserialized object
     */
    public static <T> T readClassAndObj(DataInput in) {
        return readInternal(in, null, false);
    }

    /**
     * Write an object to output, in a way that can be read by readIntoObject.
     *
     * @param out    Output stream
     * @param object Object to be written
     */
    public static void writeOutOfObject(final DataOutput out, final Object object) {
        writeInternal(out, object, true);
    }

    /**
     * Reads an object, from input, into a given object, allowing object reuse. Inverse of
     * writeOutOfObject.
     *
     * @param in     Input stream
     * @param object Object to fill from input
     */
    public static void readIntoObject(DataInput in, Object object) {
        readInternal(in, object, true);
    }

    /**
     * Writes class and object to specified output stream with specified Kryo object. It does not
     * use interim buffers.
     *
     * @param kryo   Kryo object
     * @param out    Output stream
     * @param object Object
     */
    public static void writeWithKryo(final HadoopKryo kryo, final Output out, final Object object) {
        kryo.writeClassAndObject(out, object);
        out.close();
    }

    /**
     * Write out of object with given kryo
     *
     * @param kryo   Kryo object
     * @param out    Output
     * @param object Object to write
     */
    public static void writeWithKryoOutOfObject(
            final HadoopKryo kryo, final Output out, final Object object) {
        kryo.writeOutOfObject(out, object);
        out.close();
    }

    /**
     * Reads class and object from specified input stream with specified kryo object. it does not
     * use interim buffers.
     *
     * @param kryo Kryo object
     * @param in   Input buffer
     * @param <T>  Object type parameter
     * @return Object
     */
    public static <T> T readWithKryo(final HadoopKryo kryo, final Input in) {
        T object;
        object = (T) kryo.readClassAndObject(in);
        in.close();
        return object;
    }

    /**
     * Read into object with given kryo.
     *
     * @param kryo   Kryo object
     * @param in     Input
     * @param object Object to read into
     */
    public static void readWithKryoIntoObject(
            final HadoopKryo kryo, final Input in, Object object) {
        kryo.readIntoObjectImpl(in, object);
        in.close();
    }

    /**
     * Create copy of the object, by magically recursively copying all of its fields, keeping
     * reference structures (like cycles)
     *
     * @param object Object to be copied
     * @param <T>    Type of the object
     * @return Copy of the object.
     */
    public static <T> T createCopy(final T object) {
        return KRYO_POOL.run(
                new KryoCallback<T>() {
                    @Override
                    public T execute(Kryo kryo) {
                        return kryo.copy(object);
                    }
                });
    }

    /**
     * Returns a kryo which doesn't track objects, hence serialization of recursive/nested objects
     * is not supported.
     * <p>
     * Reference tracking significantly degrades the performance since kryo has to store all
     * serialized objects and search the history to check if an object has been already serialized.
     *
     * @return Hadoop kryo which doesn't track objects.
     */
    public static HadoopKryo getNontrackingKryo() {
        return KRYO.get();
    }

    // Private implementation:

    /**
     * Create new instance of HadoopKryo, properly initialized.
     *
     * @param trackReferences if true, object references are tracked.
     * @param hasBuffer       if true, an interim buffer is used.
     * @return new HadoopKryo instance
     */
    private static HadoopKryo createKryo(boolean trackReferences, boolean hasBuffer) {
        HadoopKryo kryo;
        if (trackReferences) {
            kryo = new HadoopKryo();
        } else {
            // Only use GiraphClassResolver if it is properly initialized.
            // This is to enable test cases which use KryoSimpleWrapper
            // but don't start ZK.
            kryo =
                    new HadoopKryo(
                            GiraphClassResolver.isInitialized()
                                    ? new GiraphClassResolver()
                                    : new DefaultClassResolver(),
                            new MapReferenceResolver());
        }

        try {
            kryo.register(Class.forName("java.lang.invoke.SerializedLambda"));
            kryo.register(
                    Class.forName(
                            "com.esotericsoftware.kryo.serializers.ClosureSerializer$Closure"),
                    new ClosureSerializer());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "Trying to use Kryo on Java version "
                            + System.getProperty("java.version")
                            + ", but unable to find needed classes",
                    e);
        }

        kryo.register(Arrays.asList().getClass(), new ArraysAsListSerializer());
        kryo.register(
                Collections.nCopies(1, new Object()).getClass(),
                new CollectionsNCopiesSerializer());

        ImmutableListSerializer.registerSerializers(kryo);
        ImmutableMapSerializer.registerSerializers(kryo);
        ImmutableBiMapSerializerUtils.registerSerializers(kryo);

        // There are many fastutil classes, register them at the end,
        // so they don't use up small registration numbers
        FastUtilSerializer.registerAll(kryo);

        kryo.setInstantiatorStrategy(
                new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        SerializerFactory customSerializerFactory =
                new SerializerFactory() {
                    @SuppressWarnings("rawtypes")
                    @Override
                    public Serializer makeSerializer(Kryo kryo, final Class<?> type) {
                        for (final Entry<Class<?>, String> entry : NON_SERIALIZABLE.entrySet()) {
                            if (entry.getKey().isAssignableFrom(type)) {
                                // Allow Class object to be serialized, but not a live instance.
                                return new Serializer() {
                                    @Override
                                    public Object read(Kryo kryo, Input input, Class type) {
                                        throw new RuntimeException(
                                                "Cannot serialize "
                                                        + type
                                                        + ". Objects being serialized cannot"
                                                        + " capture "
                                                        + entry.getKey()
                                                        + " because "
                                                        + entry.getValue()
                                                        + ". Either remove field in question, or"
                                                        + " make it transient (so that it isn't"
                                                        + " serialized)");
                                    }

                                    @Override
                                    public void write(Kryo kryo, Output output, Object object) {
                                        throw new RuntimeException(
                                                "Cannot serialize "
                                                        + type
                                                        + ". Objects being serialized cannot"
                                                        + " capture "
                                                        + entry.getKey()
                                                        + " because "
                                                        + entry.getValue()
                                                        + ". Either remove field in question, or"
                                                        + " make it transient (so that it isn't"
                                                        + " serialized)");
                                    }
                                };
                            }
                        }

                        if (Writable.class.isAssignableFrom(type)
                                && !KryoIgnoreWritable.class.isAssignableFrom(type)
                                &&
                                // remove BasicSet, BasicArrayList and Basic2ObjectMap temporarily,
                                // for lack of constructors
                                !BasicSet.class.isAssignableFrom(type)
                                && !Basic2ObjectMap.class.isAssignableFrom(type)) {
                            // use the Writable method defined by the type
                            DirectWritableSerializer serializer = new DirectWritableSerializer();
                            return serializer;
                        } else {
                            FieldSerializer serializer = new FieldSerializer<>(kryo, type);
                            serializer.setIgnoreSyntheticFields(false);
                            return serializer;
                        }
                    }
                };

        kryo.addDefaultSerializer(Writable.class, customSerializerFactory);
        kryo.setDefaultSerializer(customSerializerFactory);

        if (hasBuffer) {
            kryo.input = new InputChunked(4096);
            kryo.output = new OutputChunked(4096);
            kryo.dataInputWrapperStream = new DataInputWrapperStream();
            kryo.dataOutputWrapperStream = new DataOutputWrapperStream();
        }

        if (!trackReferences) {
            kryo.setReferences(false);

            // Auto reset can only be disabled if the GiraphClassResolver is
            // properly initialized.
            if (GiraphClassResolver.isInitialized()) {
                kryo.setAutoReset(false);
            }
        }
        return kryo;
    }

    /**
     * Register serializer for class with class name
     *
     * @param kryo       HadoopKryo
     * @param className  Name of the class for which to register serializer
     * @param serializer Serializer to use
     */
    private static void registerSerializer(
            HadoopKryo kryo, String className, Serializer serializer) {
        try {
            kryo.register(Class.forName(className), serializer);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Class " + className + " is missing", e);
        }
    }

    /**
     * Internal write implementation, that reuses HadoopKryo objects from the pool.
     *
     * @param out    Output stream
     * @param object Object to be written
     * @param outOf  whether we are writing reusable objects, or full objects with class name
     */
    private static void writeInternal(
            final DataOutput out, final Object object, final boolean outOf) {
        KRYO_POOL.run(
                new KryoCallback<Void>() {
                    @Override
                    public Void execute(Kryo kryo) {
                        HadoopKryo hkryo = (HadoopKryo) kryo;
                        hkryo.setDataOutput(out);

                        if (outOf) {
                            hkryo.writeOutOfObject(hkryo.output, object);
                        } else {
                            hkryo.writeClassAndObject(hkryo.output, object);
                        }

                        hkryo.output.endChunks();
                        hkryo.output.close();

                        return null;
                    }
                });
    }

    /**
     * Internal read implementation, that reuses HadoopKryo objects from the pool.
     *
     * @param in        Input stream
     * @param outObject Object to fill from input (if not null)
     * @param into      whether we are reading reusable objects, or full objects with class name
     * @param <T>       Type of the object to read
     * @return Read object (new one, or same passed in if we use reusable)
     */
    @SuppressWarnings("unchecked")
    private static <T> T readInternal(final DataInput in, final T outObject, final boolean into) {
        return KRYO_POOL.run(
                new KryoCallback<T>() {
                    @Override
                    public T execute(Kryo kryo) {
                        HadoopKryo hkryo = (HadoopKryo) kryo;
                        hkryo.setDataInput(in);

                        T object;
                        if (into) {
                            hkryo.readIntoObjectImpl(hkryo.input, outObject);
                            object = outObject;
                        } else {
                            object = (T) hkryo.readClassAndObject(hkryo.input);
                        }
                        hkryo.input.nextChunks();

                        hkryo.input.close();
                        return object;
                    }
                });
    }

    /**
     * Initialize reusable objects for reading from given DataInput.
     *
     * @param in Input stream
     */
    private void setDataInput(DataInput in) {
        dataInputWrapperStream.setDataInput(in);
        input.setInputStream(dataInputWrapperStream);
    }

    /**
     * Initialize reusable objects for writing into given DataOutput.
     *
     * @param out Output stream
     */
    private void setDataOutput(DataOutput out) {
        dataOutputWrapperStream.setDataOutput(out);
        output.setOutputStream(dataOutputWrapperStream);
    }

    /**
     * Get or create reusable serializer for given class.
     *
     * @param type Type of the object
     * @return Serializer
     */
    private ReusableFieldSerializer<Object> getOrCreateReusableSerializer(Class<?> type) {
        ReusableFieldSerializer<Object> serializer = classToIntoSerializer.get(type);
        if (serializer == null) {
            serializer = new ReusableFieldSerializer<>(this, type);
            classToIntoSerializer.put(type, serializer);
        }
        return serializer;
    }

    /**
     * Reads an object, from input, into a given object, allowing object reuse.
     *
     * @param input  Input stream
     * @param object Object to fill from input
     */
    private void readIntoObjectImpl(Input input, Object object) {
        Preconditions.checkNotNull(object);

        Class<?> type = object.getClass();
        ReusableFieldSerializer<Object> serializer = getOrCreateReusableSerializer(type);

        serializer.setReadIntoObject(object);
        Object result = readObject(input, type, serializer);

        Preconditions.checkState(result == object);
    }

    /**
     * Write an object to output, in a way that can be read using readIntoObject.
     *
     * @param output Output stream
     * @param object Object to be written
     */
    private void writeOutOfObject(Output output, Object object) {
        ReusableFieldSerializer<Object> serializer =
                getOrCreateReusableSerializer(object.getClass());
        writeObject(output, object, serializer);
    }
}
