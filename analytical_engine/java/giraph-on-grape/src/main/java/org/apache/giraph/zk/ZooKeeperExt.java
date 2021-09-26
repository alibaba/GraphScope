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

package org.apache.giraph.zk;

import org.apache.hadoop.util.Progressable;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * ZooKeeper provides only atomic operations.  ZooKeeperExt provides additional non-atomic
 * operations that are useful.  It also provides wrappers to deal with ConnectionLossException.  All
 * methods of this class should be thread-safe.
 */
public class ZooKeeperExt {

    /**
     * Length of the ZK sequence number
     */
    public static final int SEQUENCE_NUMBER_LENGTH = 10;
    /**
     * Internal logger
     */
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperExt.class);
    /**
     * Internal ZooKeeper
     */
    private final ZooKeeper zooKeeper;
    /**
     * Ensure we have progress
     */
    private final Progressable progressable;
    /**
     * Number of max attempts to retry when failing due to connection loss
     */
    private final int maxRetryAttempts;
    /**
     * Milliseconds to wait before trying again due to connection loss
     */
    private final int retryWaitMsecs;

    /**
     * Constructor to connect to ZooKeeper, does not make progress
     *
     * @param connectString    Comma separated host:port pairs, each corresponding to a zk server.
     *                         e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If the optional
     *                         chroot suffix is used the example would look like:
     *                         "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" where the client
     *                         would be rooted at "/app/a" and all paths would be relative to this
     *                         root - ie getting/setting/etc... "/foo/bar" would result in
     *                         operations being run on "/app/a/foo/bar" (from the server
     *                         perspective).
     * @param sessionTimeout   Session timeout in milliseconds
     * @param maxRetryAttempts Max retry attempts during connection loss
     * @param retryWaitMsecs   Msecs to wait when retrying due to connection loss
     * @param watcher          A watcher object which will be notified of state changes, may also be
     *                         notified for node events
     * @throws IOException
     */
    public ZooKeeperExt(
            String connectString,
            int sessionTimeout,
            int maxRetryAttempts,
            int retryWaitMsecs,
            Watcher watcher)
            throws IOException {
        this(connectString, sessionTimeout, maxRetryAttempts, retryWaitMsecs, watcher, null);
    }

    /**
     * Constructor to connect to ZooKeeper, make progress
     *
     * @param connectString    Comma separated host:port pairs, each corresponding to a zk server.
     *                         e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If the optional
     *                         chroot suffix is used the example would look like:
     *                         "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a" where the client
     *                         would be rooted at "/app/a" and all paths would be relative to this
     *                         root - ie getting/setting/etc... "/foo/bar" would result in
     *                         operations being run on "/app/a/foo/bar" (from the server
     *                         perspective).
     * @param sessionTimeout   Session timeout in milliseconds
     * @param maxRetryAttempts Max retry attempts during connection loss
     * @param retryWaitMsecs   Msecs to wait when retrying due to connection loss
     * @param watcher          A watcher object which will be notified of state changes, may also be
     *                         notified for node events
     * @param progressable     Makes progress for longer operations
     * @throws IOException
     */
    public ZooKeeperExt(
            String connectString,
            int sessionTimeout,
            int maxRetryAttempts,
            int retryWaitMsecs,
            Watcher watcher,
            Progressable progressable)
            throws IOException {
        this.zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
        this.progressable = progressable;
        this.maxRetryAttempts = maxRetryAttempts;
        this.retryWaitMsecs = retryWaitMsecs;
    }

    /**
     * Provides a possibility of a creating a path consisting of more than one znode (not atomic).
     * If recursive is false, operates exactly the same as create().
     *
     * @param path       path to create
     * @param data       data to set on the final znode
     * @param acl        acls on each znode created
     * @param createMode only affects the final znode
     * @param recursive  if true, creates all ancestors
     * @return Actual created path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String createExt(
            final String path, byte[] data, List<ACL> acl, CreateMode createMode, boolean recursive)
            throws KeeperException, InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("createExt: Creating path " + path);
        }

        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            try {
                if (!recursive) {
                    return zooKeeper.create(path, data, acl, createMode);
                }

                try {
                    return zooKeeper.create(path, data, acl, createMode);
                } catch (KeeperException.NoNodeException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("createExt: Cannot directly create node " + path);
                    }
                }

                int pos = path.indexOf("/", 1);
                for (; pos != -1; pos = path.indexOf("/", pos + 1)) {
                    try {
                        if (progressable != null) {
                            progressable.progress();
                        }
                        String filePath = path.substring(0, pos);
                        if (zooKeeper.exists(filePath, false) == null) {
                            zooKeeper.create(filePath, null, acl, CreateMode.PERSISTENT);
                        }
                    } catch (KeeperException.NodeExistsException e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "createExt: Znode "
                                            + path.substring(0, pos)
                                            + " already exists");
                        }
                    }
                }
                return zooKeeper.create(path, data, acl, createMode);
            } catch (KeeperException.ConnectionLossException e) {
                LOG.warn(
                        "createExt: Connection loss on attempt "
                                + attempt
                                + ", "
                                + "waiting "
                                + retryWaitMsecs
                                + " msecs before retrying.",
                        e);
            }
            ++attempt;
            Thread.sleep(retryWaitMsecs);
        }
        throw new IllegalStateException(
                "createExt: Failed to create " + path + " after " + attempt + " tries!");
    }

    /**
     * Create a znode.  Set the znode if the created znode already exists.
     *
     * @param path       path to create
     * @param data       data to set on the final znode
     * @param acl        acls on each znode created
     * @param createMode only affects the final znode
     * @param recursive  if true, creates all ancestors
     * @param version    Version to set if setting
     * @return Path of created znode or Stat of set znode
     * @throws InterruptedException
     * @throws KeeperException
     */
    public PathStat createOrSetExt(
            final String path,
            byte[] data,
            List<ACL> acl,
            CreateMode createMode,
            boolean recursive,
            int version)
            throws KeeperException, InterruptedException {
        String createdPath = null;
        Stat setStat = null;
        try {
            createdPath = createExt(path, data, acl, createMode, recursive);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("createOrSet: Node exists on path " + path);
            }
            setStat = zooKeeper.setData(path, data, version);
        }
        return new PathStat(createdPath, setStat);
    }

    /**
     * Create a znode if there is no other znode there
     *
     * @param path       path to create
     * @param data       data to set on the final znode
     * @param acl        acls on each znode created
     * @param createMode only affects the final znode
     * @param recursive  if true, creates all ancestors
     * @return Path of created znode or Stat of set znode
     * @throws InterruptedException
     * @throws KeeperException
     */
    public PathStat createOnceExt(
            final String path, byte[] data, List<ACL> acl, CreateMode createMode, boolean recursive)
            throws KeeperException, InterruptedException {
        String createdPath = null;
        Stat setStat = null;
        try {
            createdPath = createExt(path, data, acl, createMode, recursive);
        } catch (KeeperException.NodeExistsException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("createOnceExt: Node already exists on path " + path);
            }
        }
        return new PathStat(createdPath, setStat);
    }

    /**
     * Delete a path recursively.  When the deletion is recursive, it is a non-atomic operation,
     * hence, not part of ZooKeeper.
     *
     * @param path      path to remove (i.e. /tmp will remove /tmp/1 and /tmp/2)
     * @param version   expected version (-1 for all)
     * @param recursive if true, remove all children, otherwise behave like remove()
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void deleteExt(final String path, int version, boolean recursive)
            throws InterruptedException, KeeperException {
        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            try {
                if (!recursive) {
                    zooKeeper.delete(path, version);
                    return;
                }

                try {
                    zooKeeper.delete(path, version);
                    return;
                } catch (KeeperException.NotEmptyException e) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("deleteExt: Cannot directly remove node " + path);
                    }
                }

                List<String> childList = zooKeeper.getChildren(path, false);
                for (String child : childList) {
                    if (progressable != null) {
                        progressable.progress();
                    }
                    deleteExt(path + "/" + child, -1, true);
                }

                zooKeeper.delete(path, version);
                return;
            } catch (KeeperException.ConnectionLossException e) {
                LOG.warn(
                        "deleteExt: Connection loss on attempt "
                                + attempt
                                + ", waiting "
                                + retryWaitMsecs
                                + " msecs before retrying.",
                        e);
            }
            ++attempt;
            Thread.sleep(retryWaitMsecs);
        }
        throw new IllegalStateException(
                "deleteExt: Failed to delete " + path + " after " + attempt + " tries!");
    }

    /**
     * Return the stat of the node of the given path. Return null if no such a node exists.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown), a watch will be
     * left on the node with the given path. The watch will be triggered by a successful operation
     * that creates/delete the node or sets the data on the node.
     *
     * @param path  the node path
     * @param watch whether need to watch this node
     * @return the stat of the node of the given path; return null if no such a node exists.
     * @throws KeeperException      If the server signals an error
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            try {
                return zooKeeper.exists(path, watch);
            } catch (KeeperException.ConnectionLossException e) {
                LOG.warn(
                        "exists: Connection loss on attempt "
                                + attempt
                                + ", waiting "
                                + retryWaitMsecs
                                + " msecs before retrying.",
                        e);
            }
            ++attempt;
            Thread.sleep(retryWaitMsecs);
        }
        throw new IllegalStateException(
                "exists: Failed to check " + path + " after " + attempt + " tries!");
    }

    /**
     * Return the stat of the node of the given path. Return null if no such a node exists.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown), a watch will be
     * left on the node with the given path. The watch will be triggered by a successful operation
     * that creates/delete the node or sets the data on the node.
     *
     * @param path    the node path
     * @param watcher explicit watcher
     * @return the stat of the node of the given path; return null if no such a node exists.
     * @throws KeeperException          If the server signals an error
     * @throws InterruptedException     If the server transaction is interrupted.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public Stat exists(final String path, Watcher watcher)
            throws KeeperException, InterruptedException {
        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            try {
                return zooKeeper.exists(path, watcher);
            } catch (KeeperException.ConnectionLossException e) {
                LOG.warn(
                        "exists: Connection loss on attempt "
                                + attempt
                                + ", waiting "
                                + retryWaitMsecs
                                + " msecs before retrying.",
                        e);
            }
            ++attempt;
            Thread.sleep(retryWaitMsecs);
        }
        throw new IllegalStateException(
                "exists: Failed to check " + path + " after " + attempt + " tries!");
    }

    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is non-null and the call is successful (no exception is thrown), a watch will be
     * left on the node with the given path. The watch will be triggered by a successful operation
     * that sets data on the node, or deletes the node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the
     * given path exists.
     *
     * @param path    the given path
     * @param watcher explicit watcher
     * @param stat    the stat of the node
     * @return the data of the node
     * @throws KeeperException          If the server signals an error with a non-zero error code
     * @throws InterruptedException     If the server transaction is interrupted.
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public byte[] getData(final String path, Watcher watcher, Stat stat)
            throws KeeperException, InterruptedException {
        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            try {
                return zooKeeper.getData(path, watcher, stat);
            } catch (KeeperException.ConnectionLossException e) {
                LOG.warn(
                        "getData: Connection loss on attempt "
                                + attempt
                                + ", waiting "
                                + retryWaitMsecs
                                + " msecs before retrying.",
                        e);
            }
            ++attempt;
            Thread.sleep(retryWaitMsecs);
        }
        throw new IllegalStateException(
                "getData: Failed to get " + path + " after " + attempt + " tries!");
    }

    /**
     * Return the data and the stat of the node of the given path.
     * <p>
     * If the watch is true and the call is successful (no exception is thrown), a watch will be
     * left on the node with the given path. The watch will be triggered by a successful operation
     * that sets data on the node, or deletes the node.
     * <p>
     * A KeeperException with error code KeeperException.NoNode will be thrown if no node with the
     * given path exists.
     *
     * @param path  the given path
     * @param watch whether need to watch this node
     * @param stat  the stat of the node
     * @return the data of the node
     * @throws KeeperException      If the server signals an error with a non-zero error code
     * @throws InterruptedException If the server transaction is interrupted.
     */
    public byte[] getData(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            try {
                return zooKeeper.getData(path, watch, stat);
            } catch (KeeperException.ConnectionLossException e) {
                LOG.warn(
                        "getData: Connection loss on attempt "
                                + attempt
                                + ", waiting "
                                + retryWaitMsecs
                                + " msecs before retrying.",
                        e);
            }
            ++attempt;
            Thread.sleep(retryWaitMsecs);
        }
        throw new IllegalStateException(
                "getData: Failed to get " + path + " after " + attempt + " tries!");
    }

    /**
     * Get the children of the path with extensions. Extension 1: Sort the children based on
     * sequence number Extension 2: Get the full path instead of relative path
     *
     * @param path           path to znode
     * @param watch          set the watch?
     * @param sequenceSorted sort by the sequence number
     * @param fullPath       if true, get the fully znode path back
     * @return list of children
     * @throws InterruptedException
     * @throws KeeperException
     */
    public List<String> getChildrenExt(
            final String path, boolean watch, boolean sequenceSorted, boolean fullPath)
            throws KeeperException, InterruptedException {
        int attempt = 0;
        while (attempt < maxRetryAttempts) {
            try {
                List<String> childList = zooKeeper.getChildren(path, watch);
                /* Sort children according to the sequence number, if desired */
                if (sequenceSorted) {
                    Collections.sort(
                            childList,
                            new Comparator<String>() {
                                public int compare(String s1, String s2) {
                                    if ((s1.length() <= SEQUENCE_NUMBER_LENGTH)
                                            || (s2.length() <= SEQUENCE_NUMBER_LENGTH)) {
                                        throw new RuntimeException(
                                                "getChildrenExt: Invalid length for sequence "
                                                        + " sorting > "
                                                        + SEQUENCE_NUMBER_LENGTH
                                                        + " for s1 ("
                                                        + s1.length()
                                                        + ") or s2 ("
                                                        + s2.length()
                                                        + ")");
                                    }
                                    int s1sequenceNumber =
                                            Integer.parseInt(
                                                    s1.substring(
                                                            s1.length() - SEQUENCE_NUMBER_LENGTH));
                                    int s2sequenceNumber =
                                            Integer.parseInt(
                                                    s2.substring(
                                                            s2.length() - SEQUENCE_NUMBER_LENGTH));
                                    return s1sequenceNumber - s2sequenceNumber;
                                }
                            });
                }
                if (fullPath) {
                    List<String> fullChildList = new ArrayList<String>();
                    for (String child : childList) {
                        fullChildList.add(path + "/" + child);
                    }
                    return fullChildList;
                }
                return childList;
            } catch (KeeperException.ConnectionLossException e) {
                LOG.warn(
                        "getChildrenExt: Connection loss on attempt "
                                + attempt
                                + ", waiting "
                                + retryWaitMsecs
                                + " msecs before retrying.",
                        e);
            }
            ++attempt;
            Thread.sleep(retryWaitMsecs);
        }
        throw new IllegalStateException(
                "createExt: Failed to create " + path + " after " + attempt + " tries!");
    }

    /**
     * Close this client object. Once the client is closed, its session becomes invalid. All the
     * ephemeral nodes in the ZooKeeper server associated with the session will be removed. The
     * watches left on those nodes (and on their parents) will be triggered.
     *
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * Data structure for handling the output of createOrSet()
     */
    public static class PathStat {

        /**
         * Path to created znode (if any)
         */
        private String path;
        /**
         * Stat from set znode (if any)
         */
        private Stat stat;

        /**
         * Put in results from createOrSet()
         *
         * @param path Path to created znode (or null)
         * @param stat Stat from set znode (if set)
         */
        public PathStat(String path, Stat stat) {
            this.path = path;
            this.stat = stat;
        }

        /**
         * Get the path of the created znode if it was created.
         *
         * @return Path of created znode or null if not created
         */
        public String getPath() {
            return path;
        }

        /**
         * Get the stat of the set znode if set
         *
         * @return Stat of set znode or null if not set
         */
        public Stat getStat() {
            return stat;
        }
    }
}
