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

package com.espro.flink.consul;

import static com.espro.flink.consul.ConsulHaConfigurationUtils.leaderPathFromConfiguration;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.concurrent.Executor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.AbstractHaServices;
import org.apache.flink.runtime.highavailability.FileSystemJobResultStore;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.NoOpJobGraphStoreWatcher;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.checkpoint.ConsulCheckpointRecoveryFactory;
import com.espro.flink.consul.jobgraph.ConsulJobGraphStoreUtil;
import com.espro.flink.consul.leader.ConsulLeaderElectionDriverFactory;
import com.espro.flink.consul.leader.ConsulLeaderRetrievalDriverFactory;

/**
 * An implementation of {@link HighAvailabilityServices} using Hashicorp Consul.
 */
public class ConsulHaServices extends AbstractHaServices {

	private static final String RESOURCE_MANAGER_LEADER_PATH = "resource_manager_lock";

	private static final String DISPATCHER_LEADER_PATH = "dispatcher_lock";

	private static final String JOB_MANAGER_LEADER_PATH = "job_manager_lock";

    private static final String REST_SERVER_LEADER_PATH = "rest_server_lock";

	/**
     * {@link ConsulClientProvider} that provides a new instance of a {@link ConsulClient} to get rid of maybe expired certificates that are renewed
     * under the hood.
     */
    private final ConsulClientProvider clientProvider;

	private final ConsulSessionActivator consulSessionActivator;

    public ConsulHaServices(Executor executor,
							Configuration configuration,
                            BlobStoreService blobStoreService,
                            ConsulClientProvider clientProvider,
                            ConsulSessionActivator consulSessionActivator) throws IOException {
        super(configuration,
                new ConsulLeaderElectionDriverFactory(clientProvider, consulSessionActivator.getHolder()),
                executor,
                blobStoreService,
                FileSystemJobResultStore.fromConfiguration(configuration, executor));
        this.clientProvider = checkNotNull(clientProvider);
		this.consulSessionActivator = checkNotNull(consulSessionActivator);
	}

    @Override
    protected String getLeaderPathForJobManager(JobID jobID) {
        return leaderPathFromConfiguration(configuration) + jobID + JOB_MANAGER_LEADER_PATH;
    }

    @Override
    protected String getLeaderPathForDispatcher() {
        return leaderPathFromConfiguration(configuration) + DISPATCHER_LEADER_PATH;
    }

    @Override
    protected String getLeaderPathForResourceManager() {
        return leaderPathFromConfiguration(configuration) + RESOURCE_MANAGER_LEADER_PATH;
    }

    @Override
    protected String getLeaderPathForRestServer() {
        return leaderPathFromConfiguration(configuration) + REST_SERVER_LEADER_PATH;
    }

    @Override
    protected LeaderRetrievalService createLeaderRetrievalService(String leaderName) {
        return new DefaultLeaderRetrievalService(new ConsulLeaderRetrievalDriverFactory(clientProvider, leaderName));
    }

    @Override
    protected CheckpointRecoveryFactory createCheckpointRecoveryFactory() throws Exception {
        return new ConsulCheckpointRecoveryFactory(clientProvider, configuration, ioExecutor);
    }

    @Override
    protected JobGraphStore createJobGraphStore() throws Exception {
        String jobgraphsPath = ConsulHaConfigurationUtils.jobGraphsPathFromConfiguration(configuration);
        Preconditions.checkArgument(jobgraphsPath.endsWith("/"), "jobgraphsPath must end with /");

        return new DefaultJobGraphStore<>(new ConsulStateHandleStore<>(clientProvider, new FileSystemStateStorageHelper<>(
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration), "jobGraph"), jobgraphsPath),
                NoOpJobGraphStoreWatcher.INSTANCE, new ConsulJobGraphStoreUtil(jobgraphsPath));
    }

	@Override
    protected void internalClose() throws Exception {
        consulSessionActivator.stop();
	}

    @Override
    protected void internalCleanup() throws Exception {
        // currently nothing to clean up
    }

    @Override
    protected void internalCleanupJobData(JobID jobID) throws Exception {
        // currently nothing to clean up
    }
}
