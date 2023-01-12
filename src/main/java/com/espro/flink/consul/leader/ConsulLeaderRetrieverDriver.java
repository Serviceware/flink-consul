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

package com.espro.flink.consul.leader;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.espro.flink.consul.ConsulUtils;

/**
 * The counterpart to the {@link ConsulLeaderElectionDriver}. {@link LeaderRetrievalService} implementation for Consul.
 * It retrieves the current leader which has been elected by the {@link ConsulLeaderElectionDriver}. The leader address
 * as well as the current leader session ID is retrieved from ZooKeeper.
 */
final class ConsulLeaderRetrieverDriver implements LeaderRetrievalDriver {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderRetrieverDriver.class);

    private final Supplier<ConsulClient> clientProvider;

    private final ScheduledExecutorService executor;

    private final String connectionInformationPath;

    private volatile boolean runnable;

    private final LeaderRetrievalEventHandler leaderRetrievalEventHandler;

    private final FatalErrorHandler fatalErrorHandler;

    private final int waitTime;

    private volatile ScheduledFuture<?> watchFuture;

    /**
     * @param clientProvider provides a Consul client
     * @param executor Executor to run background tasks
     * @param path Path of the Consul K/V store which contains the leader information
     * @param waitTime Consul blocking read timeout (in seconds)
     */
    public ConsulLeaderRetrieverDriver(Supplier<ConsulClient> clientProvider,
            String path,
            LeaderRetrievalEventHandler leaderRetrievalEventHandler,
            FatalErrorHandler fatalErrorHandler,
            int waitTime) {
        this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
        this.executor = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("consulLeaderRetrieval-pool-%d").build());
        this.connectionInformationPath = ConsulUtils.generateConnectionInformationPath(Preconditions.checkNotNull(path, "path"));
        this.leaderRetrievalEventHandler = Preconditions.checkNotNull(leaderRetrievalEventHandler, "leaderRetrievalEventHandler");
        this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler, "fatalErrorHandler");
        this.waitTime = waitTime;
    }

    public void start() {
        LOG.info("Starting Consul Leader Retriever");
        runnable = true;
        watchFuture = executor.scheduleAtFixedRate(this::watch, 0, 5, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Stopping Consul Leader Retriever");
        runnable = false;
        if (watchFuture != null) {
            watchFuture.cancel(true);
        }
        executor.shutdownNow();
    }

    private void watch() {
        if (!runnable) {
            return;
        }

        try {
            GetBinaryValue value = readConnectionInformation();
            if (runnable) {
                if (value == null) {
                    notifyNoLeader();
                    return;
                }

                byte[] leaderInformationBytes = value.getValue();
                if (leaderInformationBytes != null && leaderInformationBytes.length > 0) {
                    leaderRetrieved(ConsulUtils.bytesToLeaderInformation(leaderInformationBytes));
                } else {
                    notifyNoLeader();
                }
            }
        } catch (Exception exception) {
            fatalErrorHandler.onFatalError(exception);
            ExceptionUtils.checkInterrupted(exception);
        }
    }

    private GetBinaryValue readConnectionInformation() {
        QueryParams queryParams = QueryParams.Builder.builder()
                .setWaitTime(waitTime)
                .build();
        try {
            Response<GetBinaryValue> leaderKeyValue = clientProvider.get().getKVBinaryValue(connectionInformationPath, queryParams);
            return leaderKeyValue.getValue();
        } catch (Exception e) {
            LOG.warn("Error while reading the connection information", e);
            return null;
        }
    }

    private void leaderRetrieved(LeaderInformation leaderInformation) {
        notifyOnLeaderRetrieved(leaderInformation);
        LOG.debug("Cluster leader retrieved {}", leaderInformation);
    }

    private void notifyOnLeaderRetrieved(LeaderInformation leaderInformation) {
        try {
            leaderRetrievalEventHandler.notifyLeaderAddress(leaderInformation);
        } catch (Exception e) {
            LOG.error("Listener failed on leader retrieved notification", e);
            ExceptionUtils.checkInterrupted(e);
        }
    }

    private void notifyNoLeader() {
        leaderRetrievalEventHandler.notifyLeaderAddress(LeaderInformation.empty());
    }
}
