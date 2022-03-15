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

import static com.esotericsoftware.kryo.util.Util.getWrapperClass;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.ObjectMap;

import org.apache.giraph.zk.ZooKeeperExt;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In order to avoid writing class names to the stream, this class resolver assigns unique integers
 * to each class name and writes/reads those integers to/from the stream. Reads assume that there is
 * already a class assigned to the given integer. This resolver only assigns unique integers for
 * classes that are not explicitly registered since those classes are already assigned unique
 * integers at the time of registration. This implementation uses zookeeper to provide consistent
 * class name to ID mapping across all + nodes.
 * <p>
 * <p>
 * If resolver encounters a class name that has not been assigned to a unique integer yet, it
 * creates a class node in zookeeper under a designated path with persistent_sequential mode -
 * allowing the file name of the class node to be suffixed with an auto incremented integer. After
 * the class node is created, the resolver reads back all the nodes under the designated path and
 * uses the unique suffix as the class id. If there are duplicate entries for the same class name
 * due to some race condition, the lowest suffix is used.
 */
public class GiraphClassResolver extends DefaultClassResolver {

    /**
     * Base ID to start for class name assignments. This number has to be high enough to not
     * conflict with explicity registered class IDs.
     */
    private static final int BASE_CLASS_ID = 1000;

    /**
     * Class logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(GiraphClassResolver.class);

    /**
     * Class name to ID cache
     */
    private static Map<String, Integer> CLASS_NAME_TO_ID = new HashMap();
    /**
     * ID to class name cache
     */
    private static Map<Integer, String> ID_TO_CLASS_NAME = new HashMap();
    /**
     * Zookeeper
     */
    private static ZooKeeperExt ZK;
    /**
     * Zookeeper path for automatic class registrations
     */
    private static String KRYO_REGISTERED_CLASS_PATH;
    /**
     * Minimum class ID assigned by zookeeper sequencing
     */
    private static int MIN_CLASS_ID = -1;
    /**
     * True if the zookeeper class registration path is already created
     */
    private static boolean IS_CLASS_PATH_CREATED = false;

    /**
     * Memoized class id
     */
    private int memoizedClassId = -1;
    /**
     * Memoized class registration
     */
    private Registration memoizedClassIdValue;

    /**
     * Sets zookeeper informaton.
     *
     * @param zookeeperExt  ZookeeperExt
     * @param kryoClassPath Zookeeper directory path where class Name-ID mapping is stored.
     */
    public static void setZookeeperInfo(ZooKeeperExt zookeeperExt, String kryoClassPath) {
        ZK = zookeeperExt;
        KRYO_REGISTERED_CLASS_PATH = kryoClassPath;
    }

    /**
     * Return true of the zookeeper is initialized.
     *
     * @return True if the zookeeper is initialized.
     */
    public static boolean isInitialized() {
        return ZK != null;
    }

    /**
     * Creates a new node for the given class name. Creation mode is persistent sequential, i.e. ZK
     * will always create a new node . There could be multiple entries for the same class name but
     * since the lowest index is used, this is not a problem.
     *
     * @param className Class name
     */
    public static void createClassName(String className) {
        try {
            String path = KRYO_REGISTERED_CLASS_PATH + "/" + className;
            ZK.createExt(
                    path,
                    null,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL,
                    true);
        } catch (KeeperException e) {
            throw new IllegalStateException("Failed to create class " + className, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while creating " + className, e);
        }
    }

    /**
     * Refreshes class-ID mapping from zookeeper. Not thread safe.
     */
    public static void refreshCache() {
        if (!IS_CLASS_PATH_CREATED) {
            try {
                ZK.createOnceExt(
                        KRYO_REGISTERED_CLASS_PATH,
                        null,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT,
                        true);
                IS_CLASS_PATH_CREATED = true;
            } catch (KeeperException e) {
                throw new IllegalStateException(
                        "Failed to refresh kryo cache " + KRYO_REGISTERED_CLASS_PATH, e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(
                        "Interrupted while refreshing kryo cache " + KRYO_REGISTERED_CLASS_PATH, e);
            }
        }

        List<String> registeredList;
        try {
            registeredList = ZK.getChildrenExt(KRYO_REGISTERED_CLASS_PATH, false, true, false);
        } catch (KeeperException e) {
            throw new IllegalStateException(
                    "Failed to retrieve child nodes for " + KRYO_REGISTERED_CLASS_PATH, e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                    "Interrupted while retrieving child nodes for " + KRYO_REGISTERED_CLASS_PATH,
                    e);
        }

        for (String name : registeredList) {
            // Since these files are created with PERSISTENT_SEQUENTIAL mode,
            // Kryo appends a sequential number to their file name.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Registered class: " + name);
            }
            String className =
                    name.substring(0, name.length() - ZooKeeperExt.SEQUENCE_NUMBER_LENGTH);
            int classId =
                    Integer.parseInt(
                            name.substring(name.length() - ZooKeeperExt.SEQUENCE_NUMBER_LENGTH));

            if (MIN_CLASS_ID == -1) {
                MIN_CLASS_ID = classId;
            }

            int adjustedId = classId - MIN_CLASS_ID + BASE_CLASS_ID;
            if (CLASS_NAME_TO_ID.putIfAbsent(className, adjustedId) == null) {
                ID_TO_CLASS_NAME.put(adjustedId, className);
            }
        }
    }

    /**
     * Gets ID for the given class name.
     *
     * @param className Class name
     * @return class id Class ID
     */
    public static int getClassId(String className) {
        if (CLASS_NAME_TO_ID.containsKey(className)) {
            return CLASS_NAME_TO_ID.get(className);
        }
        synchronized (GiraphClassResolver.class) {
            if (CLASS_NAME_TO_ID.containsKey(className)) {
                return CLASS_NAME_TO_ID.get(className);
            }
            refreshCache();

            if (!CLASS_NAME_TO_ID.containsKey(className)) {
                createClassName(className);
                refreshCache();
            }
        }

        if (!CLASS_NAME_TO_ID.containsKey(className)) {
            throw new IllegalStateException("Failed to assigned id to " + className);
        }

        return CLASS_NAME_TO_ID.get(className);
    }

    /**
     * Get class name for given ID.
     *
     * @param id class ID
     * @return class name
     */
    public static String getClassName(int id) {
        if (ID_TO_CLASS_NAME.containsKey(id)) {
            return ID_TO_CLASS_NAME.get(id);
        }
        synchronized (GiraphClassResolver.class) {
            if (ID_TO_CLASS_NAME.containsKey(id)) {
                return ID_TO_CLASS_NAME.get(id);
            }
            refreshCache();
        }

        if (!ID_TO_CLASS_NAME.containsKey(id)) {
            throw new IllegalStateException("ID " + id + " doesn't exist");
        }
        return ID_TO_CLASS_NAME.get(id);
    }

    @Override
    public Registration register(Registration registration) {
        if (registration == null) {
            throw new IllegalArgumentException("registration cannot be null");
        }
        if (registration.getId() == NAME) {
            throw new IllegalArgumentException("Invalid registration ID");
        }

        idToRegistration.put(registration.getId(), registration);
        classToRegistration.put(registration.getType(), registration);
        if (registration.getType().isPrimitive()) {
            classToRegistration.put(getWrapperClass(registration.getType()), registration);
        }
        return registration;
    }

    @Override
    public Registration registerImplicit(Class type) {
        return register(
                new Registration(
                        type, kryo.getDefaultSerializer(type), getClassId(type.getName())));
    }

    @Override
    public Registration writeClass(Output output, Class type) {
        if (type == null) {
            output.writeVarInt(Kryo.NULL, true);
            return null;
        }

        Registration registration = kryo.getRegistration(type);
        if (registration.getId() == NAME) {
            throw new IllegalStateException("Invalid registration ID");
        } else {
            // Class ID's are incremented by 2 when writing, because 0 is used
            // for null and 1 is used for non-explicitly registered classes.
            output.writeVarInt(registration.getId() + 2, true);
        }
        return registration;
    }

    @Override
    public Registration readClass(Input input) {
        int classID = input.readVarInt(true);
        if (classID == Kryo.NULL) {
            return null;
        } else if (classID == NAME + 2) {
            throw new IllegalStateException("Invalid class ID");
        }
        if (classID == memoizedClassId) {
            return memoizedClassIdValue;
        }
        Registration registration = idToRegistration.get(classID - 2);
        if (registration == null) {
            String className = getClassName(classID - 2);
            Class type = getTypeByName(className);
            if (type == null) {
                try {
                    type = Class.forName(className, false, kryo.getClassLoader());
                } catch (ClassNotFoundException ex) {
                    throw new KryoException("Unable to find class: " + className, ex);
                }
                if (nameToClass == null) {
                    nameToClass = new ObjectMap();
                }
                nameToClass.put(className, type);
            }
            registration = new Registration(type, kryo.getDefaultSerializer(type), classID - 2);
            register(registration);
        }
        memoizedClassId = classID;
        memoizedClassIdValue = registration;
        return registration;
    }

    /**
     * Reset the internal state Reset clears two hash tables: 1 - Class name to ID: Every
     * non-explicitly registered class takes the ID agreed by all kryo instances, and it doesn't
     * change across serializations, so this reset is not required. 2- Reference tracking: Not
     * required because it is disabled.
     * <p>
     * Therefore, this method should not be invoked.
     */
    public void reset() {
        throw new IllegalStateException("Not implemented");
    }

    /**
     * This method writes the class name for the first encountered non-explicitly registered class.
     * Since all non-explicitly registered classes take the ID agreed by all kryo instances, there
     * is no need to write the class name, so this method should not be invoked.
     *
     * @param output       Output stream
     * @param type         CLass type
     * @param registration Registration
     */
    @Override
    protected void writeName(Output output, Class type, Registration registration) {
        throw new IllegalStateException("Not implemented");
    }

    /**
     * This method reads the class name for the first encountered non-explicitly registered class.
     * Since all non-explicitly registered classes take the ID agreed by all kryo instances, class
     * name is never written, so this method should not be invoked.
     *
     * @param input Input stream
     * @return Registration
     */
    @Override
    protected Registration readName(Input input) {
        throw new IllegalStateException("Not implemented");
    }

    /**
     * Get type by class name.
     *
     * @param className Class name
     * @return class type
     */
    protected Class<?> getTypeByName(final String className) {
        return nameToClass != null ? nameToClass.get(className) : null;
    }
}
