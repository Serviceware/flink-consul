/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.leader;

import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriver.Listener;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriverFactory;

import com.espro.flink.consul.ConsulClientProvider;
import com.espro.flink.consul.ConsulSessionHolder;


/**
 * Factory for {@link ConsulLeaderElectionDriver}.
 */
public class ConsulLeaderElectionDriverFactory implements MultipleComponentLeaderElectionDriverFactory {

    private final ConsulClientProvider clientProvider;
    private final ConsulSessionHolder sessionHolder;
    private final String leaderBasePath;

    public ConsulLeaderElectionDriverFactory(ConsulClientProvider clientProvider, ConsulSessionHolder sessionHolder,
            String leaderBasePath) {
        this.clientProvider = clientProvider;
        this.sessionHolder = sessionHolder;
        this.leaderBasePath = leaderBasePath;
    }

    @Override
    public MultipleComponentLeaderElectionDriver create(Listener leaderElectionListener) throws Exception {
        return new ConsulLeaderElectionDriver(clientProvider, sessionHolder, leaderBasePath, leaderElectionListener);
    }

}
