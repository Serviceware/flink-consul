package com.espro.flink.consul.checkpoint;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.espro.flink.consul.TestUtil;
import com.espro.flink.consul.metric.ConsulMetricGroup;
import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.AbstractConsulTest;

public class ConsulCheckpointIDCounterTest extends AbstractConsulTest {

	private ConsulClient client;
	private String countersPath = "test-counters/";
	private ConsulMetricService consulMetricService;

	@Before
	public void setup() {
		client = new ConsulClient("localhost", consul.getHttpPort());
		Configuration configuration = new Configuration();
		MetricRegistry metricRegistry = TestUtil.createMetricRegistry(configuration);
		ConsulMetricGroup consulMetricGroup = new ConsulMetricGroup(metricRegistry, configuration.getString(JobManagerOptions.BIND_HOST));
		this.consulMetricService = new ConsulMetricService(metricRegistry, consulMetricGroup);
	}

	@Test
	public void testGetAndIncrement() throws Exception {
		JobID jobID = JobID.generate();
        ConsulCheckpointIDCounter counter = new ConsulCheckpointIDCounter(() -> client, countersPath, jobID, consulMetricService);
		counter.start();
		assertEquals(0, counter.getAndIncrement());
		assertEquals(1, counter.getAndIncrement());
		counter.shutdown(JobStatus.FINISHED);
	}

	@Test
	public void testSetCount() throws Exception {
		JobID jobID = JobID.generate();
        ConsulCheckpointIDCounter counter = new ConsulCheckpointIDCounter(() -> client, countersPath, jobID, consulMetricService);
		counter.start();
		counter.setCount(3);
		assertEquals(3, counter.getAndIncrement());
		counter.shutdown(JobStatus.FINISHED);
	}

	@Test
	public void testSharedAccess() throws Exception {
		JobID jobID = JobID.generate();
        ConsulCheckpointIDCounter counter1 = new ConsulCheckpointIDCounter(() -> client, countersPath, jobID, consulMetricService);
        ConsulCheckpointIDCounter counter2 = new ConsulCheckpointIDCounter(() -> client, countersPath, jobID, consulMetricService);

		counter1.start();
		counter2.start();

		assertEquals(0, counter1.getAndIncrement());
		assertEquals(1, counter1.getAndIncrement());
		assertEquals(2, counter2.getAndIncrement());

		counter1.shutdown(JobStatus.FINISHED);
		counter2.shutdown(JobStatus.FINISHED);
	}

	@Test
	public void testConcurrentAccess() throws Exception {
		JobID jobID = JobID.generate();
        ConsulCheckpointIDCounter counter1 = new ConsulCheckpointIDCounter(() -> client, countersPath, jobID, consulMetricService);
        ConsulCheckpointIDCounter counter2 = new ConsulCheckpointIDCounter(() -> client, countersPath, jobID, consulMetricService);

		counter1.start();
		counter2.start();

		Runnable task1 = () -> {
			try {
				for (int i = 0; i < 10; i++) {
					counter1.getAndIncrement();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		Runnable task2 = () -> {
			try {
				for (int i = 0; i < 10; i++) {
					counter2.getAndIncrement();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		Executor executor = Executors.newFixedThreadPool(2);
		executor.execute(task1);
		executor.execute(task2);

		Thread.sleep(1000);

		assertEquals(20, counter1.getAndIncrement());
		assertEquals(21, counter2.getAndIncrement());

		counter1.shutdown(JobStatus.FINISHED);
		counter2.shutdown(JobStatus.FINISHED);
	}


	@Test
	public void testStop() throws Exception {
		JobID jobID = JobID.generate();
        ConsulCheckpointIDCounter counter = new ConsulCheckpointIDCounter(() -> client, countersPath, jobID, consulMetricService);
		counter.start();
		counter.getAndIncrement();
		counter.getAndIncrement();
		counter.shutdown(JobStatus.FINISHED);
		assertEquals(0, counter.getAndIncrement());
	}
}
