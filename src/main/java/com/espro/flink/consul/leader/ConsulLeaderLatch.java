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

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.espro.flink.consul.ConsulSessionHolder;

final class ConsulLeaderLatch {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderLatch.class);

    private final Supplier<ConsulClient> clientProvider;

    private final ScheduledExecutorService executor;

	private final ConsulSessionHolder sessionHolder;

	private final String leaderKey;

	/**
     * Unique identifier for this leader latch
     */
	private UUID leaderIdentifier;

	private long leaderKeyIndex;

	private volatile boolean runnable;

	private boolean hasLeadership;

	private final ConsulLeaderLatchListener listener;

    private final int watchIntervalInSeconds;

    private volatile ScheduledFuture<?> watchFuture;

	/**
     * @param clientProvider provides a Consul client
     * @param executor Executor to run background tasks
     * @param leaderKey key in Consul KV store
     * @param nodeAddress leadership changes are reported to this contender
     * @param watchIntervalInSeconds watch interval for leader election (in seconds)
     */
    public ConsulLeaderLatch(Supplier<ConsulClient> clientProvider,
							 ConsulSessionHolder sessionHolder,
							 String leaderKey,
							 ConsulLeaderLatchListener listener,
							 int watchIntervalInSeconds) {
		this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
        this.executor = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("consulLeaderLatch-pool-%d").build());
		this.sessionHolder = Preconditions.checkNotNull(sessionHolder, "sessionHolder");
		this.leaderKey = Preconditions.checkNotNull(leaderKey, "leaderKey");
		this.listener = Preconditions.checkNotNull(listener, "listener");
        this.watchIntervalInSeconds = watchIntervalInSeconds;
	}

	public void start() {
        leaderIdentifier = UUID.randomUUID();
        LOG.info("Starting Consul Leadership Latch using leader identifier: {}", leaderIdentifier);
		runnable = true;
        watchFuture = executor.scheduleAtFixedRate(this::watch, 0, watchIntervalInSeconds, TimeUnit.SECONDS);
	}

	public void stop() {
		LOG.info("Stopping Consul Leadership Latch");
		runnable = false;
		hasLeadership = false;
        if (watchFuture != null) {
            watchFuture.cancel(true);
        }
        releaseLeaderKey();
        executor.shutdownNow();
	}

	private void watch() {
        if (!runnable) {
            return;
        }

        try {
            GetBinaryValue value = readLeaderKey();
            String leaderSessionId = null;
            if (value != null) {
                leaderKeyIndex = value.getModifyIndex();
                leaderSessionId = value.getSession();
            }

            if (runnable && leaderSessionId == null) {
                LOG.info("No leader elected. Current node is trying to register");
                boolean success = writeLeaderKey();
                if (success) {
                    leadershipAcquired();
                } else {
                    leadershipRevoked();
				}
			}
        } catch (Exception e) {
            LOG.warn("Exception during leadership election", e);
		}
	}

    UUID getLeaderIdentifier() {
        return leaderIdentifier;
    }

    public boolean hasLeadership() {
        return hasLeadership;
	}

	private GetBinaryValue readLeaderKey() {
        QueryParams queryParams = QueryParams.Builder.builder()
                .setIndex(leaderKeyIndex)
                .setWaitTime(watchIntervalInSeconds)
                .build();
        Response<GetBinaryValue> leaderKeyValue = clientProvider.get().getKVBinaryValue(leaderKey, queryParams);
		return leaderKeyValue.getValue();
	}

    private boolean writeLeaderKey() {
		PutParams putParams = new PutParams();
		putParams.setAcquireSession(sessionHolder.getSessionId());
		try {
            ConsulLeaderData data = ConsulLeaderData.from(leaderIdentifier);
            Boolean response = clientProvider.get().setKVBinaryValue(leaderKey, data.toBytes(), putParams).getValue();
            return response != null && response;
		} catch (OperationException ex) {
            LOG.error("Error while writing leader identifier {} to Consul.", leaderIdentifier, ex);
			return false;
		}
	}

	private Boolean releaseLeaderKey() {
		PutParams putParams = new PutParams();
		putParams.setReleaseSession(sessionHolder.getSessionId());
		try {
            return clientProvider.get().setKVBinaryValue(leaderKey, new byte[0], putParams).getValue();
		} catch (OperationException ex) {
            LOG.error("Error while releasing leader identifier {} for consul session {}.", leaderIdentifier, sessionHolder.getSessionId(),
                    ex);
			return false;
		}
	}

    private void leadershipAcquired() {
		if (!hasLeadership) {
			hasLeadership = true;
            listener.isLeader();
            LOG.info("Cluster leadership has been acquired from current node with leader identifier {}", leaderIdentifier);
		}
	}

	private void leadershipRevoked() {
		if (hasLeadership) {
			hasLeadership = false;
            listener.notLeader();
            LOG.info("Cluster leadership has been revoked from current node with leader identifier {}", leaderIdentifier);
		}
	}
}
