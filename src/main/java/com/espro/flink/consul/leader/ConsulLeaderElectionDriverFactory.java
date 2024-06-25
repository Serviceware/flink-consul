/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.leader;

import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.LeaderElectionDriverFactory;

import com.espro.flink.consul.ConsulClientProvider;
import com.espro.flink.consul.ConsulSessionHolder;


/**
 * Factory for {@link ConsulLeaderElectionDriver}.
 */
public class ConsulLeaderElectionDriverFactory implements LeaderElectionDriverFactory {

    private final ConsulClientProvider clientProvider;
    private final ConsulSessionHolder sessionHolder;

    public ConsulLeaderElectionDriverFactory(ConsulClientProvider clientProvider, ConsulSessionHolder sessionHolder) {
        this.clientProvider = clientProvider;
        this.sessionHolder = sessionHolder;
    }

    @Override
    public LeaderElectionDriver create(LeaderElectionDriver.Listener leaderElectionListener) throws Exception {
        return new ConsulLeaderElectionDriver(clientProvider, sessionHolder, leaderElectionListener);
    }

}
