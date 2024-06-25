/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.leader;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espro.flink.consul.ConsulClientProvider;
import com.espro.flink.consul.ConsulSessionHolder;
import com.espro.flink.consul.ConsulUtils;

/**
 * Consul based {@link MultipleComponentLeaderElectionDriver} implementation.
 */
public class ConsulLeaderElectionDriver implements LeaderElectionDriver, ConsulLeaderLatchListener {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulLeaderElectionDriver.class);

    private final LeaderElectionDriver.Listener leaderElectionListener;
    private final ConsulLeaderLatch leaderLatch;

    private final ConsulClientProvider clientProvider;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public ConsulLeaderElectionDriver(ConsulClientProvider clientProvider, ConsulSessionHolder sessionHolder,
            LeaderElectionDriver.Listener leaderElectionListener) {
        this.clientProvider = clientProvider;
        this.leaderElectionListener = leaderElectionListener;

        this.leaderLatch = new ConsulLeaderLatch(clientProvider, sessionHolder, ConsulUtils.getLeaderLatchPath(), this, 2);
        leaderLatch.start();
    }

    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            leaderLatch.stop();
        }
    }

    @Override
    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }

    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation) {
        Preconditions.checkState(running.get());

        if (!leaderLatch.hasLeadership()) {
            return;
        }

        String componentPath = ConsulUtils.generateConnectionInformationPath(componentId);

        try {
            final byte[] data;
            if (leaderInformation.isEmpty()) {
                data = null;
            } else {
                data = ConsulUtils.leaderInformationToBytes(leaderInformation);
            }

            boolean dataWritten = false;
            while (!dataWritten && leaderLatch.hasLeadership()) {
                Boolean response = clientProvider.executeWithSslRecovery(consulClient -> consulClient.setKVBinaryValue(componentPath, data).getValue());
                dataWritten = response != null && response;
            }
        } catch (Exception e) {
            leaderElectionListener.onError(e);
        }
    }

    @Override
    public void deleteLeaderInformation(String componentId) {
        String componentPath = ConsulUtils.generateConsulPath(componentId);
        clientProvider.executeWithSslRecovery(consulClient -> consulClient.deleteKVValue(componentPath));
    }

    @Override
    public void isLeader() {
        final UUID leaderSessionID = UUID.randomUUID();
        LOG.debug("{} obtained the leadership with session ID {}.", this, leaderSessionID);
        leaderElectionListener.onGrantLeadership(leaderSessionID);
    }

    @Override
    public void notLeader() {
        LOG.debug("{} lost the leadership.", this);
        leaderElectionListener.onRevokeLeadership();
    }

}
