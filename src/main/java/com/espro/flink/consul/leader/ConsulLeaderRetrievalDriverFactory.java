/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.leader;

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriverFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import com.espro.flink.consul.ConsulClientProvider;

/**
 * {@link LeaderRetrievalDriverFactory} implementation for Consul.
 */
public class ConsulLeaderRetrievalDriverFactory implements LeaderRetrievalDriverFactory {

    private final ConsulClientProvider clientProvider;
    private final String leaderKey;

    public ConsulLeaderRetrievalDriverFactory(ConsulClientProvider clientProvider, String leaderKey) {
        this.clientProvider = clientProvider;
        this.leaderKey = leaderKey;
    }

    @Override
    public LeaderRetrievalDriver createLeaderRetrievalDriver(LeaderRetrievalEventHandler leaderEventHandler,
            FatalErrorHandler fatalErrorHandler) throws Exception {
        ConsulLeaderRetrieverDriver consulLeaderRetrieverDriver = new ConsulLeaderRetrieverDriver(clientProvider, leaderKey,
                leaderEventHandler, fatalErrorHandler, 10);
        consulLeaderRetrieverDriver.start();
        return consulLeaderRetrieverDriver;
    }

}
