package com.espro.flink.consul;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;

public class ConsulSessionActivatorTest extends AbstractConsulTest {

    /**
     * @see ConsulHighAvailabilityOptions#HA_CONSUL_SESSION_TTL
     */
    private static final int SESSION_TTL_DEFAULT = 10;

    private ConsulClient consulClient;

	@Before
	public void setup() {
        consulClient = new ConsulClient("localhost", consul.getHttpPort());
	}

	@Test
	public void testSessionLifecycle() throws Exception {
        ConsulClient spiedClient = spy(consulClient);
        ConsulSessionActivator cse = new ConsulSessionActivator(new ConsulClientProviderImpl(spiedClient), new Configuration());

        // WHEN session activator is started
        ConsulSessionHolder holder = cse.start();

        // THEN session is created
        Awaitility.await().catchUncaughtExceptions().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> verify(spiedClient).sessionCreate(any(NewSession.class), any(QueryParams.class)));
        assertNotNull(consulClient.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());

        // THEN session is renewed to keep it
        Awaitility.await().catchUncaughtExceptions().atMost(Duration.ofSeconds(SESSION_TTL_DEFAULT + 1))
                .untilAsserted(() -> verify(spiedClient).renewSession(anyString(), any(QueryParams.class)));
        assertNotNull(consulClient.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());

        // WHEN session activator is stopped
		cse.stop();

        // THEN session is destroyed
        Awaitility.await().catchUncaughtExceptions().atMost(Duration.ofSeconds(SESSION_TTL_DEFAULT + 1))
                .untilAsserted(() -> verify(spiedClient).sessionDestroy(anyString(), any(QueryParams.class)));
        assertNull(consulClient.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());
	}

}
