package com.espro.flink.consul.jobgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collection;

import com.espro.flink.consul.TestUtil;
import com.espro.flink.consul.metric.ConsulMetricGroup;
import com.espro.flink.consul.metric.ConsulMetricService;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.util.FlinkException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.AbstractConsulTest;

public class ConsulSubmittedJobGraphStoreTest extends AbstractConsulTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ConsulClient client;
    private Configuration configuration;
	private String jobgraphsPath = "test-jobgraphs/";
	private ConsulMetricService consulMetricService;


	@Before
    public void setup() throws IOException {
		client = new ConsulClient("localhost", consul.getHttpPort());

        // Provide a HA_STORAGE_PATH
        configuration = new Configuration();
        configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpFolder.newFolder().getAbsolutePath());
		MetricRegistry metricRegistry = TestUtil.createMetricRegistry(configuration);
		ConsulMetricGroup consulMetricGroup = new ConsulMetricGroup(metricRegistry, configuration.getString(JobManagerOptions.BIND_HOST));
        this.consulMetricService = new ConsulMetricService(consulMetricGroup);
	}

	@Test
	public void testPutAndRecoverJobGraph() throws Exception {
        ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath, consulMetricService);
        ConsulSubmittedJobGraphStore graphStore2 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath, consulMetricService);

        JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		graphStore2.start(listener);
		JobID jobID = JobID.generate();

        JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		verify(listener).onAddedJobGraph(jobID);
		graphStore1.stop();

        JobGraph recoverJobGraph = graphStore2.recoverJobGraph(jobID);
        assertEquals(jobGraph.getJobID(), recoverJobGraph.getJobID());
		assertNotSame(jobGraph, recoverJobGraph);
	}

	@Test(expected = FlinkException.class)
	public void testPutAndRemoveJobGraph() throws Exception {
        ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath, consulMetricService);

        JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

        JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);
		graphStore1.removeJobGraph(jobID);
		verify(listener).onRemovedJobGraph(jobID);

		graphStore1.recoverJobGraph(jobID);
	}

	@Test
	public void testGetJobIds() throws Exception {
        ConsulSubmittedJobGraphStore graphStore1 = new ConsulSubmittedJobGraphStore(configuration, () -> client, jobgraphsPath, consulMetricService);

        JobGraphStore.JobGraphListener listener = mock(JobGraphStore.JobGraphListener.class);

		graphStore1.start(listener);
		JobID jobID = JobID.generate();

        JobGraph jobGraph = createJobGraph(jobID);
		graphStore1.putJobGraph(jobGraph);

		Collection<JobID> jobIds = graphStore1.getJobIds();
		assertEquals(1, jobIds.size());
		assertEquals(jobID, jobIds.iterator().next());
	}

    private JobGraph createJobGraph(JobID jobID) {
        return new JobGraph(jobID, "test-job");
	}
}
