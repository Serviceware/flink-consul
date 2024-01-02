package com.espro.flink.consul;

import static com.espro.flink.consul.configuration.ConsulHighAvailabilityOptions.HA_CONSUL_SESSION_TTL;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.esotericsoftware.minlog.Log;

/**
 * Keeps Consul session active.
 */
public final class ConsulSessionActivator {

	private static final Logger LOG = LoggerFactory.getLogger(ConsulSessionActivator.class);

    private final ConsulClientProvider clientProvider;
    private final ScheduledExecutorService executorService;
    private final Duration sessionTtl;
	private volatile boolean running;
	private final ConsulSessionHolder holder = new ConsulSessionHolder();

	/**
     * @param clientProvider provides Consul client
     * @param executor runs session keep-alive background task
     * @param sessionTtl session ttl in seconds
     */
    public ConsulSessionActivator(ConsulClientProvider clientProvider, Configuration configuration) {
        this.clientProvider = Preconditions.checkNotNull(clientProvider, "client");
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.sessionTtl = Duration.ofSeconds(configuration.getInteger(HA_CONSUL_SESSION_TTL));
	}

	public ConsulSessionHolder start() {
        LOG.info("Starting ConsulSessionActivator");
        running = true;
        executorService.execute(this::doRun);
		return holder;
	}

	public void stop() {
		running = false;

        // There is no need to wait for execution of scheduled tasks
        LOG.info("Shuting down now executorService...");
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        destroyConsulSession();
        LOG.info("Stopped ConsulSessionActivator");
	}

	private void doRun() {
        if (running) {
            createOrRenewConsulSession();
        }
	}

    private void scheduleRenewalOfSession() {
        // Consul session is renewed after 80% of the ttl is passed
        executorService.schedule(this::doRun, (long) (sessionTtl.toMillis() * 0.8), TimeUnit.MILLISECONDS);
    }

    private void scheduleCreationOfSession() {
        executorService.schedule(this::doRun, 100, TimeUnit.MILLISECONDS);
    }

	private void createOrRenewConsulSession() {
		if (holder.getSessionId() == null) {
			createConsulSession();
		} else {
			renewConsulSession();
		}
	}

	private void createConsulSession() {
        try {
            NewSession newSession = new NewSession();
            newSession.setName("flink");
            newSession.setTtl(String.format("%ds", Math.max(10, sessionTtl.toMillis() / 1000)));
            holder.setSessionId(clientProvider.executeWithSslRecovery(consulClient -> consulClient.sessionCreate(newSession, QueryParams.DEFAULT).getValue()));
            Log.info("New consul session is created {}", holder.getSessionId());
            scheduleRenewalOfSession();
        } catch (Exception e) {
            LOG.error("Error while creating new consul session", e);
            scheduleCreationOfSession();
        }
	}

	private void renewConsulSession() {
		try {
            clientProvider.executeWithSslRecovery(consulClient -> consulClient.renewSession(holder.getSessionId(), QueryParams.DEFAULT));
            scheduleRenewalOfSession();
        } catch (OperationException e) {
            LOG.warn("Consul session renew failed, a new session is created.", e);
            createConsulSession();
        } catch (Exception e) {
			LOG.error("Consul session renew failed", e);
            scheduleCreationOfSession();
		}
	}

	private void destroyConsulSession() {
		try {
            clientProvider.executeWithSslRecovery(consulClient -> consulClient.sessionDestroy(holder.getSessionId(), QueryParams.DEFAULT));
		} catch (Exception e) {
			LOG.error("Consul session destroy failed", e);
		}
	}

	public ConsulSessionHolder getHolder() {
		return holder;
	}
}
