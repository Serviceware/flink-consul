package com.espro.flink.consul;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.espro.flink.consul.metric.ConsulMetricGroup;
import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;

public class ConsulSessionActivatorTest extends AbstractConsulTest {

	private ConsulClient client;
	private ConsulMetricService consulMetricService;

	@Before
	public void setup() {
		client = new ConsulClient("localhost", consul.getHttpPort());
		Configuration configuration = new Configuration();
		MetricRegistry metricRegistry = TestUtil.createMetricRegistry(configuration);
		ConsulMetricGroup consulMetricGroup = new ConsulMetricGroup(metricRegistry, configuration.getString(JobManagerOptions.BIND_HOST));
		this.consulMetricService = new ConsulMetricService(consulMetricGroup);
	}

	@Test
	public void testSessionLifecycle() throws Exception {
		ConsulClient spiedClient = spy(client);
        ConsulSessionActivator cse = new ConsulSessionActivator(() -> spiedClient, 10, consulMetricService);
		ConsulSessionHolder holder = cse.start();
		Thread.sleep(1000);

		verify(spiedClient).sessionCreate(any(NewSession.class), any(QueryParams.class));

		assertNotNull(client.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());
		Thread.sleep(11000);

		verify(spiedClient).renewSession(anyString(), any(QueryParams.class));
		assertNotNull(client.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());

		cse.stop();

		Thread.sleep(11000);
		verify(spiedClient).sessionDestroy(anyString(), any(QueryParams.class));
		assertNull(client.getSessionInfo(holder.getSessionId(), QueryParams.DEFAULT).getValue());
	}

}
