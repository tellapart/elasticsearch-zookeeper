/*
 * Copyright 2011 Sonian Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.sonian.elasticsearch.zookeeper.discovery;

import com.sonian.elasticsearch.zookeeper.client.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.DiscoveryNodesProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Publishes and retrieves cluster state using zookeeper.
 * <p/>
 * By default cluster state is stored in /es/elasticsearch/state node. State is published in several parts.
 * Each part is updated only if it got changed and then /es/elasticsearch/state/state node is updated to
 * reflect the latest versions of the parts.
 *
 * @author imotov
 */
public class ZooKeeperClusterState extends AbstractLifecycleComponent<ZooKeeperClusterState> {

    private final Lock publishingLock = new ReentrantLock();

    private final ZooKeeperClient zooKeeperClient;

    private final ZooKeeperEnvironment environment;

    private final List<ClusterStatePart<?>> parts = new ArrayList<ClusterStatePart<?>>();

    private final DiscoveryNodesProvider nodesProvider;

    private final ClusterName clusterName;

    private Version localVersion = Version.CURRENT;

    private volatile boolean watching = true;

    public ZooKeeperClusterState(Settings settings, ZooKeeperEnvironment environment, ZooKeeperClient zooKeeperClient, DiscoveryNodesProvider nodesProvider, ClusterName clusterName) {
        this(settings, environment, zooKeeperClient, nodesProvider, clusterName, null);
    }

    public ZooKeeperClusterState(Settings settings, ZooKeeperEnvironment environment, ZooKeeperClient zooKeeperClient, DiscoveryNodesProvider nodesProvider, ClusterName clusterName, Version clusterStateVersion) {
        super(settings);
        this.zooKeeperClient = zooKeeperClient;
        this.environment = environment;
        this.nodesProvider = nodesProvider;
        this.clusterName = clusterName;
        initClusterStatePersistence();
        if (null != clusterStateVersion) {
            this.localVersion = clusterStateVersion;
        }
    }


    /**
     * Publishes new cluster state
     *
     * @param state
     * @throws org.elasticsearch.ElasticsearchException
     *
     * @throws InterruptedException
     */
    public void publish(ClusterState state, Discovery.AckListener ackListener) throws ElasticsearchException, InterruptedException {
        // TODO: Add ack logic
        publish(state/*, new AckClusterStatePublishResponseHandler(state.nodes().size() - 1, ackListener)*/ );
        ackListener.onTimeout();
    }

    private void publish(ClusterState state/*, final ClusterStatePublishResponseHandler publishResponseHandler*/) throws ElasticsearchException, InterruptedException {
        publishingLock.lock();
        try {
            logger.trace("Publishing new cluster state version [{}]", state.version());
            // Make sure state node exists
            zooKeeperClient.createPersistentNode(environment.stateNodePath());
            final String statePath = environment.statePartsNodePath();
            final BytesStreamOutput buf = new BytesStreamOutput();
            writeVersion(buf);
            buf.writeLong(state.version());
            for (ClusterStatePart<?> part : this.parts) {
                buf.writeString(part.publishClusterStatePart(state));
            }
            zooKeeperClient.setOrCreatePersistentNode(statePath, buf.bytes().copyBytesArray().toBytes());

            // Cleaning previous versions of updated cluster parts
            for (ClusterStatePart<?> part : this.parts) {
                part.purge();
            }
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot publish state", e);
        } finally {
            publishingLock.unlock();
        }

    }

    /**
     * Retrieves cluster state
     *
     * @param newClusterStateListener triggered when cluster state changes
     * @return
     * @throws ElasticsearchException
     * @throws InterruptedException
     */
    public ClusterState retrieve(final NewClusterStateListener newClusterStateListener) throws ElasticsearchException, InterruptedException {
        publishingLock.lock();
        try {
            if (!lifecycle.started()) {
                return null;
            }
            logger.trace("Retrieving new cluster state");
            if (newClusterStateListener != null) {
                watching = true;
            } else {
                watching = false;
            }
            final String statePath = environment.statePartsNodePath();
            ZooKeeperClient.NodeListener nodeListener;
            if (newClusterStateListener != null) {
                nodeListener = new AbstractNodeListener() {
                    @Override
                    public void onNodeCreated(String id) {
                        if (watching) {
                            updateClusterState(newClusterStateListener);
                        }
                    }

                    @Override
                    public void onNodeDataChanged(String id) {
                        if (watching) {
                            updateClusterState(newClusterStateListener);
                        }
                    }
                };
            } else {
                nodeListener = null;
            }
            byte[] stateBuf = zooKeeperClient.getNode(statePath, nodeListener);
            if (stateBuf == null) {
                return null;
            }
            final BytesStreamInput buf = new BytesStreamInput(stateBuf);
            Version stateVersion;
            try {
                stateVersion = Version.readVersion(buf);
                if (stateVersion.major == 0 && stateVersion.minor == 0) {
                    stateVersion = readStringVersion(buf);
                }
            } catch (IOException e) {
                stateVersion = readStringVersion(buf);
            }
            buf.setVersion(stateVersion);
            if (!stateVersion.equals(localVersion()) && !Version
                    .largest(localVersion(), stateVersion).minimumCompatibilityVersion()
                    .onOrBefore(Version.smallest(localVersion(), stateVersion))) {
                throw new ZooKeeperIncompatibleStateVersionException(
                        "Local version: " + localVersion()
                                + " incompatible with remote version: " + stateVersion);
            }

            ClusterState.Builder builder = ClusterState.builder(clusterName).version(buf.readLong());
            for (ClusterStatePart<?> part : this.parts) {
                builder = part.set(builder, buf.readString(), stateVersion);
                if (builder == null) {
                    return null;
                }
            }

            return builder.build();
        } catch (IOException e) {
            throw new ZooKeeperClientException("Cannot retrieve state", e);
        } finally {
            publishingLock.unlock();
        }

    }

    /**
     * For backwards compatibility on upgrade read version serialized by old versions of this plugin,
     * which were in the form Version.number() or, for plugin versions 1.3.1+,
     * (Version.CURRENT.major).append('.').append(Version.CURRENT.minor)
     * @param buf
     * @return
     */
    private Version readStringVersion(StreamInput buf) throws IOException {
        buf.reset();
        String versionStr = buf.readString();
        try {
            return Version.fromString(versionStr);
        } catch (IllegalArgumentException ee) {
            //approximate exact version for upgrade from state which only had version in form Major.Minor
            return Version.fromString(versionStr + ".0");
        }
    }

    /**
     * Makes sure that internal cache structures are in sync with zookeeper
     * <p/>
     * This method should be called when node becomes master and switches from retrieving cluster state
     * to publishing cluster state.
     */
    public void syncClusterState() throws ElasticsearchException, InterruptedException {
        // To prepare for publishing master state, make sure that we are in sync with zooKeeper
        try {
            retrieve(null);
        } catch (ZooKeeperIncompatibleStateVersionException ex) {
            logger.info("Incompatible version of state found - cleaning. {}", ex.getMessage());
            cleanClusterStateNode();
        }
    }

    private void cleanClusterStateNode() throws ElasticsearchException, InterruptedException {
        Set<String> parts = zooKeeperClient.listNodes(environment.stateNodePath(), null);
        for (String part : parts) {
            // Don't delete the part node itself. Other nodes might already have watchers set on this node
            if (!"parts".equals(part)) {
                 zooKeeperClient.deleteNodeRecursively(environment.stateNodePath() + "/" + part);
            }
        }
    }

    private void updateClusterState(NewClusterStateListener newClusterStateListener) {
        try {
            ClusterState clusterState = retrieve(newClusterStateListener);
            if (clusterState != null) {
                newClusterStateListener.onNewClusterState(clusterState);
            }
        } catch (ZooKeeperClientSessionExpiredException ex) {
            // Ignore session should be restarted
        } catch (Exception ex) {
            logger.error("Error updating cluster state", ex);
        }
    }

    @Override
    protected void doStart() throws ElasticsearchException {
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    protected Version localVersion() {
        return localVersion;
    }

    protected void writeVersion(StreamOutput out) throws IOException {
        Version.writeVersion(localVersion(), out);
    }

    public interface NewClusterStateListener {
        public void onNewClusterState(ClusterState clusterState);
    }

    // TODO: this logic should be moved to the actual classes that represent parts of Cluster State after zookeeper-
    // based discovery is merged to master.
    private void initClusterStatePersistence() {
        parts.add(new ClusterStatePart<RoutingTable>("routingTable") {
            @Override
            public void writeTo(RoutingTable statePart, StreamOutput out) throws IOException {
                RoutingTable.Builder.writeTo(statePart, out);
            }

            @Override
            public RoutingTable readFrom(StreamInput in) throws IOException {
                return RoutingTable.Builder.readFrom(in);
            }

            @Override
            public RoutingTable get(ClusterState state) {
                return state.getRoutingTable();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, RoutingTable val) {
                return builder.routingTable(val);
            }
        });
        parts.add(new ClusterStatePart<DiscoveryNodes>("discoveryNodes") {
            @Override
            public void writeTo(DiscoveryNodes statePart, StreamOutput out) throws IOException {
                DiscoveryNodes.Builder.writeTo(statePart, out);
            }

            @Override
            public DiscoveryNodes readFrom(StreamInput in) throws IOException {
                return DiscoveryNodes.Builder.readFrom(in, nodesProvider.nodes().localNode());
            }

            @Override
            public DiscoveryNodes get(ClusterState state) {
                return state.getNodes();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, DiscoveryNodes val) {
                return builder.nodes(val);
            }
        });
        parts.add(new ClusterStatePart<MetaData>("metaData") {
            @Override
            public void writeTo(MetaData statePart, StreamOutput out) throws IOException {
                MetaData.Builder.writeTo(statePart, out);
            }

            @Override
            public MetaData readFrom(StreamInput in) throws IOException {
                return MetaData.Builder.readFrom(in);
            }

            @Override
            public MetaData get(ClusterState state) {
                return state.metaData();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, MetaData val) {
                return builder.metaData(val);
            }
        });
        parts.add(new ClusterStatePart<ClusterBlocks>("clusterBlocks") {
            @Override
            public void writeTo(ClusterBlocks statePart, StreamOutput out) throws IOException {
                ClusterBlocks.Builder.writeClusterBlocks(statePart, out);
            }

            @Override
            public ClusterBlocks readFrom(StreamInput in) throws IOException {
                return ClusterBlocks.Builder.readClusterBlocks(in);
            }

            @Override
            public ClusterBlocks get(ClusterState state) {
                return state.blocks();
            }

            @Override
            public ClusterState.Builder set(ClusterState.Builder builder, ClusterBlocks val) {
                return builder.blocks(val);
            }
        });
    }

    private abstract class ClusterStatePart<T> {
        private final String statePartName;

        private T cached;

        private String cachedPath;

        private String previousPath;

        private Version cachedVersion;

        public ClusterStatePart(String statePartName) {
            this.statePartName = statePartName;
        }

        public String publishClusterStatePart(ClusterState state) throws ElasticsearchException, InterruptedException {
            T statePart = get(state);
            if (statePart.equals(cached) && localVersion().equals(cachedVersion)) {
                return cachedPath;
            } else {
                String path = internalPublishClusterStatePart(statePart);
                cached = statePart;
                previousPath = cachedPath;
                cachedPath = path;
                cachedVersion = localVersion();
                return path;
            }
        }

        private String internalPublishClusterStatePart(T statePart) throws ElasticsearchException, InterruptedException {
            final String path = environment.stateNodePath() + "/" + statePartName + "_";
            String rootPath;
            try {
                BytesStreamOutput streamOutput = new BytesStreamOutput();
                streamOutput.setVersion(localVersion());
                writeTo(statePart, streamOutput);
                // Create Root node with version and size of the state part
                rootPath = zooKeeperClient.createLargeSequentialNode(path, streamOutput.bytes().copyBytesArray().toBytes());
            } catch (IOException e) {
                throw new ZooKeeperClientException("Cannot read " + statePartName + " node at " + path, e);
            }
            return rootPath;
        }

        public T getClusterStatePart(String path, Version version) throws ElasticsearchException, InterruptedException {
            if (path.equals(cachedPath)) {
                return cached;
            } else {
                T part = internalGetStatePart(path, version);
                if (part != null) {
                    cached = part;
                    cachedPath = path;
                    cachedVersion = version;
                    return cached;
                } else {
                    return null;
                }
            }

        }

        public void purge() throws ElasticsearchException, InterruptedException {
            if (previousPath != null) {
                try {
                    zooKeeperClient.deleteLargeNode(previousPath);
                } catch (ZooKeeperClientException ex) {
                    // It's possible that ZooKeeper lost all data - ignore this error
                    logger.trace("Error deleting node");
                }
                previousPath = null;
            }
        }

        public T internalGetStatePart(final String path, Version version) throws ElasticsearchException, InterruptedException {
            try {

                byte[] buf = zooKeeperClient.getLargeNode(path);
                if (buf != null) {
                    StreamInput in = new BytesStreamInput(buf);
                    in.setVersion(version);
                    return readFrom(in);
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new ZooKeeperClientException("Cannot read " + statePartName + " node at " + path, e);
            }
        }

        public ClusterState.Builder set(ClusterState.Builder builder, String path, Version version) throws ElasticsearchException, InterruptedException {
            T val = getClusterStatePart(path, version);
            if (val == null) {
                return null;
            } else {
                return set(builder, val);
            }

        }

        public abstract void writeTo(T statePart, StreamOutput out) throws IOException;

        public abstract T readFrom(StreamInput in) throws IOException;

        public abstract T get(ClusterState state);

        public abstract ClusterState.Builder set(ClusterState.Builder builder, T val);

    }

}
