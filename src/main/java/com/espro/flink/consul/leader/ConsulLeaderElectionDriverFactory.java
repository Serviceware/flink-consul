/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.leader;

import java.util.function.Supplier;

import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriver;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriver.Listener;
import org.apache.flink.runtime.leaderelection.MultipleComponentLeaderElectionDriverFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.ConsulSessionHolder;


/**
 * Factory for {@link ConsulLeaderElectionDriver}.
 */
public class ConsulLeaderElectionDriverFactory implements MultipleComponentLeaderElectionDriverFactory {

    private final Supplier<ConsulClient> clientProvider;
    private final ConsulSessionHolder sessionHolder;

    public ConsulLeaderElectionDriverFactory(Supplier<ConsulClient> clientProvider, ConsulSessionHolder sessionHolder) {
        this.clientProvider = clientProvider;
        this.sessionHolder = sessionHolder;
    }

    @Override
    public MultipleComponentLeaderElectionDriver create(Listener leaderElectionListener) throws Exception {
        return new ConsulLeaderElectionDriver(clientProvider, sessionHolder, leaderElectionListener);
    }

}
