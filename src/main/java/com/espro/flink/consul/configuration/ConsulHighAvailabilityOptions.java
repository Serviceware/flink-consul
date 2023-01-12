package com.espro.flink.consul.configuration;

import static org.apache.flink.configuration.ConfigOptions.key;

import org.apache.flink.configuration.ConfigOption;

import com.ecwid.consul.transport.TLSConfig.KeyStoreInstanceType;

/**
 * Holds all possible configuration properties related to properly configure the HA Consul integration.
 */
public final class ConsulHighAvailabilityOptions {

	/**
	 * The root path under which Flink stores its entries in Consul
	 */
    public static final ConfigOption<String> HA_CONSUL_ROOT = key("high-availability.consul.path.root")
            .stringType()
            .defaultValue("flink/");

	/**
	 * Consul root path for job graphs.
	 */
    public static final ConfigOption<String> HA_CONSUL_JOBGRAPHS_PATH = key("high-availability.consul.path.jobgraphs")
            .stringType()
            .defaultValue("jobgraphs/");

	/**
	 * Consul root path for job status.
	 */
    public static final ConfigOption<String> HA_CONSUL_JOBSTATUS_PATH = key("high-availability.consul.path.jobstatus")
            .stringType()
            .defaultValue("jobstatus/");

    public static final ConfigOption<String> HA_CONSUL_LEADER_PATH = key("high-availability.consul.path.leader")
            .stringType()
            .defaultValue("leader/");

	/**
	 * Consul root path for completed checkpoints.
	 */
    public static final ConfigOption<String> HA_CONSUL_CHECKPOINTS_PATH = key("high-availability.consul.path.checkpoints")
            .stringType()
            .defaultValue("checkpoints/");

	/**
	 * Consul root path for checkpoint counters.
	 */
    public static final ConfigOption<String> HA_CONSUL_CHECKPOINT_COUNTER_PATH = key("high-availability.consul.path.checkpoint-counter")
            .stringType()
            .defaultValue("checkpoint-counter/");

    /**
     * Defines the consul host to connect to.
     */
    public static final ConfigOption<String> HA_CONSUL_HOST = key("high-availability.consul.host")
            .stringType()
            .defaultValue("localhost");

    /**
     * Defines the consul port to connect to.
     */
    public static final ConfigOption<Integer> HA_CONSUL_PORT = key("high-availability.consul.port")
            .intType()
            .defaultValue(8550);

    /**
     * Defines the consul session ttl in seconds.
     */
    public static final ConfigOption<Integer> HA_CONSUL_SESSION_TTL = key("high-availability.consul.session.ttl")
            .intType()
            .defaultValue(10);

    /**
     * Enables the use of tls secured connections to consul.
     */
    public static final ConfigOption<Boolean> HA_CONSUL_TLS_ENABLED = key("high-availability.consul.tls.enabled")
            .booleanType()
            .defaultValue(false);

    /**
     * Defines the path to the keystore.
     */
    public static final ConfigOption<String> HA_CONSUL_TLS_KEYSTORE_PATH = key("high-availability.consul.tls.keystore.path")
            .stringType()
            .noDefaultValue();

    /**
     * Defines the password of the keystore.
     */
    public static final ConfigOption<String> HA_CONSUL_TLS_KEYSTORE_PASSWORD = key("high-availability.consul.tls.keystore.password")
            .stringType()
            .noDefaultValue();

    /**
     * Defines the keystore type.
     *
     * @see KeyStoreInstanceType
     */
    public static final ConfigOption<String> HA_CONSUL_TLS_KEYSTORE_TYPE = key("high-availability.consul.tls.keystore.type")
            .stringType()
            .defaultValue("PKCS12");

    /**
     * Defines the path to the truststore.
     */
    public static final ConfigOption<String> HA_CONSUL_TLS_TRUSTSTORE_PATH = key("high-availability.consul.tls.truststore.path")
            .stringType()
            .noDefaultValue();

    /**
     * Defines the password of the truststore.
     */
    public static final ConfigOption<String> HA_CONSUL_TLS_TRUSTSTORE_PASSWORD = key("high-availability.consul.tls.truststore.password")
            .stringType()
            .noDefaultValue();

    /**
     * Defines the truststore type.
     *
     * @see KeyStoreInstanceType
     */
    public static final ConfigOption<String> HA_CONSUL_TLS_TRUSTSTORE_TYPE = key("high-availability.consul.tls.truststore.type")
            .stringType()
            .defaultValue("PKCS12");

    /**
     * Defines the algorithm that is used for the ssl context.
     *
     * @see https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext
     */
    public static final ConfigOption<String> HA_CONSUL_TLS_ALGORITHM = key("high-availability.consul.tls.algorithm")
            .stringType()
            .defaultValue("TLS");

    /**
     * Read timeout in seconds
     */
    public static final ConfigOption<Integer> HA_CONSUL_CLIENT_CONNECT_TIMEOUT = key("high-availability.consul.client.connect-timeout")
            .intType()
            .defaultValue(10);

    /**
     * Connection request timeout in seconds
     */
    public static final ConfigOption<Integer> HA_CONSUL_CLIENT_CONNECTION_REQUEST_TIMEOUT = key("high-availability.consul.client.connection-request-timeout")
            .intType()
            .defaultValue(10);

    /**
     * Socket timeout in seconds
     */
    public static final ConfigOption<Integer> HA_CONSUL_CLIENT_SOCKET_TIMEOUT = key("high-availability.consul.client.socket-timeout")
            .intType()
            .defaultValue(30);

    private ConsulHighAvailabilityOptions() {
        // class for holding constants
    }
}
