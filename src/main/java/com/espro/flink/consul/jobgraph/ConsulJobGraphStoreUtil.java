/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.jobgraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.JobGraphStoreUtil;


/**
 * Singleton {@link JobGraphStoreUtil} implementation for Consul.
 */
public enum ConsulJobGraphStoreUtil implements JobGraphStoreUtil {

    INSTANCE;

    @Override
    public String jobIDToName(JobID jobId) {
        checkNotNull(jobId, "Job ID");
        return String.format("/%s", jobId);
    }

    @Override
    public JobID nameToJobID(String name) {
        return JobID.fromHexString(name);
    }

}
