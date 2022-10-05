/*
 * Copyright (c) SABIO GmbH, Hamburg 2022 - All rights reserved
 */
package com.espro.flink.consul.jobgraph;

import static org.junit.Assert.assertEquals;

import org.apache.flink.api.common.JobID;
import org.junit.Test;

/**
 *
 */
public class ConsulJobGraphStoreUtilTest {

    @Test
    public void testJobIDToName() {
        // GIVEN path to job graph
        String givenJobGraphPath = "path/to/my/jobGraphes/";

        // GIVEN instance of class
        ConsulJobGraphStoreUtil consulJobGraphStoreUtil = new ConsulJobGraphStoreUtil(givenJobGraphPath);

        // WHEN converting the job id into a name
        JobID jobId = JobID.generate();
        String actualName = consulJobGraphStoreUtil.jobIDToName(jobId);

        // THEN the name is as expected
        String expectedName = givenJobGraphPath + jobId.toString();
        assertEquals(expectedName, actualName);
    }

    @Test
    public void testNameToJobID() {
        // GIVEN path to job graph
        String givenJobGraphPath = "my/jobGraphes/";

        // GIVEN instance of class
        ConsulJobGraphStoreUtil consulJobGraphStoreUtil = new ConsulJobGraphStoreUtil(givenJobGraphPath);

        // WHEN converting the name into a job id
        JobID expectedJobId = JobID.generate();
        String name = givenJobGraphPath + expectedJobId.toString();
        JobID actualJobId = consulJobGraphStoreUtil.nameToJobID(name);

        // THEN the job id is as expected
        assertEquals(expectedJobId, actualJobId);
    }
}
