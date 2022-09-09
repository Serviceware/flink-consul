/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.espro.flink.consul.leader;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ecwid.consul.v1.ConsulClient;
import com.espro.flink.consul.AbstractConsulTest;
import com.espro.flink.consul.ConsulSessionActivator;
import com.espro.flink.consul.ConsulSessionHolder;

public class ConsulLeaderLatchTest extends AbstractConsulTest {

	private ConsulClient client;
	private int waitTime = 1;
	private ConsulSessionActivator sessionActivator1;
	private ConsulSessionHolder sessionHolder1;
	private ConsulSessionActivator sessionActivator2;
	private ConsulSessionHolder sessionHolder2;

	@Before
	public void setup() {
		client = new ConsulClient(String.format("localhost:%d", consul.getHttpPort()));
        sessionActivator1 = new ConsulSessionActivator(() -> client, 10);
		sessionHolder1 = sessionActivator1.start();
        sessionActivator2 = new ConsulSessionActivator(() -> client, 10);
		sessionHolder2 = sessionActivator2.start();
	}

	@After
	public void cleanup() {
        if (sessionActivator1 != null) {
            sessionActivator1.stop();
        }
        if (sessionActivator2 != null) {
            sessionActivator2.stop();
        }
	}

	@Test
    public void testLeaderElection() throws Exception {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener = mock(ConsulLeaderLatchListener.class);

        ConsulLeaderLatch latch = new ConsulLeaderLatch(() -> client, sessionHolder1, leaderKey, listener, waitTime);
		latch.start();

        awaitLeaderElection();
        verify(listener).isLeader();

        assertTrue(latch.hasLeadership());

		latch.stop();
	}

	@Test
    public void testLeaderElectionTwoNodes() throws Exception {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener1 = mock(ConsulLeaderLatchListener.class);
		ConsulLeaderLatchListener listener2 = mock(ConsulLeaderLatchListener.class);

        ConsulLeaderLatch latch1 = new ConsulLeaderLatch(() -> client, sessionHolder1, leaderKey, listener1, waitTime);
        ConsulLeaderLatch latch2 = new ConsulLeaderLatch(() -> client, sessionHolder2, leaderKey, listener2, waitTime);

		latch1.start();
        awaitLeaderElection();

		latch2.start();
        awaitLeaderElection();

        verify(listener1).isLeader();
        assertTrue(latch1.hasLeadership());
        assertFalse(latch2.hasLeadership());

		latch1.stop();
        awaitLeaderElection();
        verify(listener2).isLeader();
        assertFalse(latch1.hasLeadership());
        assertTrue(latch2.hasLeadership());

		latch2.stop();
        assertFalse(latch1.hasLeadership());
        assertFalse(latch2.hasLeadership());
	}

	@Test
    public void testConsulReset() throws Exception {
		String leaderKey = "test-key";

		ConsulLeaderLatchListener listener = mock(ConsulLeaderLatchListener.class);

        ConsulLeaderLatch latch = new ConsulLeaderLatch(() -> client, sessionHolder1, leaderKey, listener, waitTime);
		latch.start();

        awaitLeaderElection();
        verify(listener).isLeader();
        assertTrue(latch.hasLeadership());

		consul.reset();
		Thread.sleep(1000 * waitTime);
        verify(listener).notLeader();
        assertFalse(latch.hasLeadership());

		latch.stop();
	}

    private void awaitLeaderElection() throws Exception {
        TimeUnit.SECONDS.sleep(2 * waitTime);
    }
}
