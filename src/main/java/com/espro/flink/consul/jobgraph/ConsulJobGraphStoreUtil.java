/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.jobgraph;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.JobGraphStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link JobGraphStoreUtil} implementation for Consul.
 */
public class ConsulJobGraphStoreUtil implements JobGraphStoreUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConsulJobGraphStoreUtil.class);

    private final String jobGraphPath;

    public ConsulJobGraphStoreUtil(String jobGraphPath) {
        checkNotNull(jobGraphPath, "job graph path");
        this.jobGraphPath = StringUtils.removeEnd(jobGraphPath, "/");
    }

    @Override
    public String jobIDToName(JobID jobId) {
        checkNotNull(jobId, "Job ID");
        return String.format("%s/%s", jobGraphPath, jobId);
    }

    @Override
    public JobID nameToJobID(String name) {
        LOG.debug("Name {} for job graph is converted to job id.", name);
        String nameWithoutBasePath = name.replace(jobGraphPath, "");
        return JobID.fromHexString(nameWithoutBasePath.replace("/", ""));
    }
}
