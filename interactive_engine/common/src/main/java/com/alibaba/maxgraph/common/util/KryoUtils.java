/**
 * Part of this is referred from project cuba,
 *
 * https://github.com/cuba-platform/cuba/blob/master/modules/global/src/com/haulmont/cuba/core/sys/serialization/KryoSerialization.java
 *
 * which has the following license:
 *
 * Copyright (c) 2008-2016 Haulmont.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.javakaffee.kryoserializers.*;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * IO util to help doing object serde thing by Kryo.
 * <p/>
 * More kryo usage see https://github.com/EsotericSoftware/kryo
 *
 * @author: ruoang, baofeng
 * @date: 15/4/28
 * @time: 下午5:52
 */
@SuppressWarnings("ALL")
public class KryoUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KryoUtils.class);

    // Kryo is not thread safe. Each thread should have its own Kryo, Input, and Output instances.
    // Also, the byte[] Input uses may be modified and then returned to its original state during
    // deserialization, so the same byte[] should not be used concurrently in separate threads.
    private static ThreadLocal<Kryo> local = ThreadLocal.withInitial(() -> newKyroInstance());
    private static ThreadLocal<String> localClassLoaderId = ThreadLocal.withInitial(() -> "");

    private static final Class<? extends List> ARRAYS_AS_LIST_CLAZZ = Arrays.asList().getClass();

    public static Kryo createDefaultKryo() {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        ImmutableListSerializer.registerSerializers(kryo);
        ImmutableSetSerializer.registerSerializers(kryo);
        ImmutableMapSerializer.registerSerializers(kryo);
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
        kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
        kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
        kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
        kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
        kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
        kryo.register(ARRAYS_AS_LIST_CLAZZ, new ArraysAsListSerializer());

        return kryo;
    }

    private static Kryo newKyroInstance() {
        Kryo kryo = createDefaultKryo();
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        ImmutableListSerializer.registerSerializers(kryo);
        ImmutableSetSerializer.registerSerializers(kryo);
        ImmutableMapSerializer.registerSerializers(kryo);
        kryo.register(ARRAYS_AS_LIST_CLAZZ, new ArraysAsListSerializer());
        return kryo;
    }

    public static byte[] obj2Bytes(Object o) throws IOException {
        return obj2Bytes(o, true, false);
    }

    /**
     * Serialize object to byte array .
     *
     * @param o          the object to serialize
     * @param writeClass if write class info in serialized byte array
     * @param compress   if the serialized byte array should be compressed
     * @return serialized byte array
     * @throws IOException if compression fails
     */
    public static byte[] obj2Bytes(Object o, boolean writeClass, boolean compress) throws IOException {
        byte[] bytes = obj2OutputStream(o, writeClass).toByteArray();
        if (compress) {
            bytes = Snappy.compress(bytes);
        }
        return bytes;
    }

    /**
     * Serialize object to byte array WITHOUT compression.
     *
     * @param o          the object to serialize
     * @param writeClass if write class info in serialized byte array
     * @return serialized byte array
     */
    public static byte[] obj2Bytes(Object o, boolean writeClass) {
        return obj2OutputStream(o, writeClass).toByteArray();
    }

    /**
     * Deserialize byte array to java object.
     *
     * @param data       the byte array to deserialize
     * @param readClass  if read class info from byte array
     * @param clazz      null if {@code readClass} is true, otherwise provide the class info
     * @param decompress whether the byte array should be  decompressed before deserialization
     * @return the deserialized java object
     * @throws IOException if  decompression fails
     */
    public static Object bytes2Object(byte[] data, boolean readClass, Class clazz, boolean decompress)
            throws IOException {
        Kryo kryo = local.get();
        if (decompress) {
            data = Snappy.uncompress(data);
        }
        Input input = new Input(data);
        if (readClass) {
            return kryo.readClassAndObject(input);
        } else {
            return kryo.readObject(input, clazz);
        }
    }

    /**
     * Deserialize byte array to java object WITHOUT decompression
     *
     * @param data      the byte array to deserialize
     * @param readClass if read class info from byte array
     * @param clazz     null if {@code readClass} is true, otherwise provide the class info
     * @return
     */
    public static Object bytes2Object(byte[] data, boolean readClass, Class clazz) {
        Kryo kryo = local.get();
        Input input = new Input(data);
        if (readClass) {
            return kryo.readClassAndObject(input);
        } else {
            return kryo.readObject(input, clazz);
        }
    }

    public static Object bytes2Object(byte[] data) {
        return bytes2Object(data, true, null);
    }

    private static ByteArrayOutputStream obj2OutputStream(Object o, boolean writeClass) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Kryo kryo = local.get();
        Output output = new Output(outputStream);
        if (writeClass) {
            kryo.writeClassAndObject(output, o);
        } else {
            kryo.writeObject(output, o);
        }
        output.flush();
        output.clear();
        output.close();
        return outputStream;
    }

    /**
     * Reset ClassLoader for Kryo -
     *
     * @param classLoader   to be loaded classLoader
     * @param classLoaderId classLoaderId - If this value does not match localClassLoaderId
     *                      renew Kyro before setting classloader -
     *                      This renew process is necessary since once class
     *                      has been registered, it cannot be replaced.
     */
    public static void resetClassLoader(ClassLoader classLoader, String classLoaderId) {
        LOG.debug("Reset Kryo Class Loader {}", classLoader.toString());
        if (!localClassLoaderId.get().equals(classLoaderId)) {
            local.remove();
            local.set(newKyroInstance());
            localClassLoaderId.set(classLoaderId);
        }
        Kryo kryo = local.get();
        kryo.setClassLoader(classLoader);
    }

    public static void remove() {
        local.remove();
        localClassLoaderId.remove();
    }

}
