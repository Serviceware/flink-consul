package com.espro.flink.consul.leader;

import static com.espro.flink.consul.ConsulUtils.generateConnectionInformationPath;
import static com.espro.flink.consul.ConsulUtils.leaderInformationToBytes;
import static org.apache.flink.runtime.leaderelection.LeaderInformation.empty;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.UUID;

import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.AbstractConsulTest;

/**
 * Verifies that the {@link LeaderRetrievalEventHandler} is called correctly by {@link ConsulLeaderRetrieverDriver}
 */
public class ConsulLeaderRetrieverDriverTest extends AbstractConsulTest {

    private static final String CONSUL_KV_PATH = "path-to-leader-information";

    private ConsulLeaderRetrieverDriver consulLeaderRetrieverDriver;

    private LeaderRetrievalEventHandler leaderRetrievalEventHandler;
    private FatalErrorHandler fatalErrorHandler;

    private ConsulClient client;

    @Before
    public void setup() {
        leaderRetrievalEventHandler = Mockito.mock(LeaderRetrievalEventHandler.class);
        fatalErrorHandler = Mockito.mock(FatalErrorHandler.class);

        client = new ConsulClient("localhost", consul.getHttpPort());

        consulLeaderRetrieverDriver = new ConsulLeaderRetrieverDriver(() -> client, CONSUL_KV_PATH, leaderRetrievalEventHandler,
                fatalErrorHandler, 2);
        consulLeaderRetrieverDriver.start();
    }

    @After
    public void shutdown() throws Exception {
        if (consulLeaderRetrieverDriver != null) {
            consulLeaderRetrieverDriver.close();
        }
    }

    @Test
    public void testNoLeader() {
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> verify(leaderRetrievalEventHandler, times(4)).notifyLeaderAddress(empty()));

        verify(fatalErrorHandler, never()).onFatalError(any(Throwable.class));
    }

    @Test
    public void testSuccessfullyRetrievedLeaderInformation() throws Exception {
        LeaderInformation expectedLeaderInformation = LeaderInformation.known(UUID.randomUUID(), UUID.randomUUID().toString());
        writeLeaderInformation(expectedLeaderInformation);

        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> verify(leaderRetrievalEventHandler, atLeastOnce()).notifyLeaderAddress(expectedLeaderInformation));

        verify(fatalErrorHandler, never()).onFatalError(any(Throwable.class));
    }

    private void writeLeaderInformation(LeaderInformation leaderInformation) throws Exception {
        Boolean response = client
                .setKVBinaryValue(generateConnectionInformationPath(CONSUL_KV_PATH), leaderInformationToBytes(leaderInformation))
                .getValue();
        if (response == null || !response) {
            Assert.fail("Unable to write leader information!");
        }
    }
}
