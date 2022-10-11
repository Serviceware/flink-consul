/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.leader;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriver;
import org.apache.flink.util.Preconditions;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.ConsulSessionHolder;
import com.espro.flink.consul.ConsulUtils;

/**
 * Consul based {@link MultipleComponentLeaderElectionDriver} implementation.
 */
public class ConsulLeaderElectionDriver implements MultipleComponentLeaderElectionDriver, ConsulLeaderLatchListener {

    private final MultipleComponentLeaderElectionDriver.Listener leaderElectionListener;
    private final ConsulLeaderLatch leaderLatch;
    private final Supplier<ConsulClient> clientProvider;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public ConsulLeaderElectionDriver(Supplier<ConsulClient> clientProvider, ConsulSessionHolder sessionHolder, String leaderBasePath,
            MultipleComponentLeaderElectionDriver.Listener leaderElectionListener) {
        this.clientProvider = clientProvider;
        this.leaderElectionListener = leaderElectionListener;

        this.leaderLatch = new ConsulLeaderLatch(clientProvider, sessionHolder, ConsulUtils.getLeaderLatchPath(leaderBasePath), this, 2);
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
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation) throws Exception {
        Preconditions.checkState(running.get());

        if (!leaderLatch.hasLeadership()) {
            return;
        }

        String componentPath = ConsulUtils.generateConnectionInformationPath(componentId);

        final byte[] data;
        if (leaderInformation.isEmpty()) {
            data = null;
        } else {
            data = ConsulUtils.leaderInformationToBytes(leaderInformation);
        }

        boolean dataWritten = false;
        while (!dataWritten && leaderLatch.hasLeadership()) {
            Boolean response = clientProvider.get().setKVBinaryValue(componentPath, data).getValue();
            dataWritten = response != null && response;
        }
    }

    @Override
    public void deleteLeaderInformation(String componentId) throws Exception {
        String componentPath = ConsulUtils.generateConsulPath(componentId);
        clientProvider.get().deleteKVValue(componentPath);
    }

    @Override
    public void isLeader() {
        leaderElectionListener.isLeader();
    }

    @Override
    public void notLeader() {
        leaderElectionListener.notLeader();
    }

}
