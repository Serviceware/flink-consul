package com.espro.flink.consul;

import static java.util.Arrays.asList;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;

/**
 * This test class starts a HA Flink cluster using the {@link ConsulHaServicesFactory} for creating a high available cluster.
 */
public class ConsulHaServicesFactoryTest extends AbstractConsulTest {

    /**
     * Consul version that is used to start the embedded consul process.
     */
    private static final String CONSUL_VERSION = "1.8.4";

    @ClassRule
    public static TemporaryFolder tmpFolder = new TemporaryFolder();

    private static ConsulProcess consul;
    private static MiniClusterWithClientResource miniClusterWithClientResource;

    @BeforeClass
    public static void start() throws Exception {
        consul = ConsulStarterBuilder.consulStarter()
                .withConsulVersion(CONSUL_VERSION)
                .build()
                .start();

        Configuration configuration = new Configuration();
        configuration.setString(HighAvailabilityOptions.HA_MODE, ConsulHaServicesFactory.class.getName());
        configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, tmpFolder.toString());
        configuration.setString(ConsulHighAvailabilityOptions.HA_CONSUL_HOST, "localhost");
        configuration.setInteger(ConsulHighAvailabilityOptions.HA_CONSUL_PORT, consul.getHttpPort());

        MiniClusterResourceConfiguration miniClusterResourceConfiguration = new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(2)
                .setNumberTaskManagers(1)
                .setConfiguration(configuration)
                .build();

        miniClusterWithClientResource = new MiniClusterWithClientResource(miniClusterResourceConfiguration);
        miniClusterWithClientResource.before();
    }

    @AfterClass
    public static void shutdown() {
        miniClusterWithClientResource.after();
        consul.close();
    }

    @Test
    public void testRunSimpleProgramInHighAvailableCluster() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure test environment
        env.setParallelism(2);
        env.enableCheckpointing(10);

        // values are collected in a static variable
        CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        env.fromElements(1L, 21L, 22L)
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertTrue(CollectSink.values.containsAll(asList(1L, 21L, 22L)));
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Long> {

        private static final long serialVersionUID = -5399234551109239048L;

        // must be static
        public static final List<Long> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Long value, SinkFunction.Context context) throws Exception {
            values.add(value);
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
