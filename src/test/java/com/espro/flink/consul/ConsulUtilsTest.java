package com.espro.flink.consul;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.junit.Test;

public class ConsulUtilsTest {

    @Test
    public void tesSerializationAndDeserializationOfLeaderInformation() throws Exception {
        LeaderInformation leaderInformation = LeaderInformation.known(UUID.randomUUID(), UUID.randomUUID().toString());

        LeaderInformation actual = ConsulUtils.bytesToLeaderInformation(ConsulUtils.leaderInformationToBytes(leaderInformation));

        assertEquals(leaderInformation.getLeaderSessionID(), actual.getLeaderSessionID());
        assertEquals(leaderInformation.getLeaderAddress(), actual.getLeaderAddress());
    }

    @Test
    public void testGenerateConnectionInformationPath() {
        assertEquals("componentId/connection_info", ConsulUtils.generateConnectionInformationPath("componentId"));
    }

    @Test
    public void testGetLeaderLatchPath() {
        assertEquals("nomad/labs-knowledge-bi-flink/leader/latch", ConsulUtils.getLeaderLatchPath("/nomad/labs-knowledge-bi-flink/leader/"));
    }
}
