/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fusesource.support.consumer;


import java.util.Properties;

import javax.jms.Destination;
import javax.jms.Session;

import org.apache.activemq.brokerservice.runner.HubSpokeBrokerServiceSpawner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Claudio Corsi
 *
 */
public class StompConsumerExecutorTest extends BaseConsumerExecutorTestSupport {

	private static final Logger logger = LoggerFactory.getLogger(StompConsumerExecutorTest.class);
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		spawner = new HubSpokeBrokerServiceSpawner() {

			private int port;
			private int hubPort;
			private int hubIdx;

			@Override
			protected void populateProperties(TYPE type, Properties props,
					int idx) {
				switch (type) {
				case HUB:
					hubPort = port++;
					hubIdx = idx;
					props.setProperty("hub.suffix.name", String.valueOf(idx));
					props.setProperty("kahadb.dir", "hub");
					props.setProperty("kahadb.prefix", String.valueOf(idx));
					props.setProperty("hub.hostname", "localhost");
					props.setProperty("hub.port.number", String.valueOf(hubPort));
					logger.info("Hub properties: " + props);
					break;
				case SPOKE:
					props.setProperty("spoke.suffix.name", String.valueOf(hubIdx) + "-" + String.valueOf(idx));
					props.setProperty("kahadb.dir", "spoke");
					props.setProperty("kahadb.prefix", String.valueOf(hubIdx) + "-" + String.valueOf(idx));
					props.setProperty("hub.hostname", "localhost");
					props.setProperty("hub.port.number", String.valueOf(hubPort));
					props.setProperty("spoke.hostname", "localhost");
					props.setProperty("spoke.port.number", String.valueOf(port++));
					logger.info("Spoke properties: " + props);
					break;
				}
			}

			@Override
			protected String getHubTemplatePrefix() {
				return "hub-activemq";
			}

			@Override
			protected String getSpokeTemplatePrefix() {
				return "stomp-spoke-activemq";
			}

                        @Override
                        protected void preCreateSpawners() {
				port = this.getAmqPort();
				logger.info("port=" + port);
                        }

			{
                            // logger.info("INSIDE INITIALIZATION");
				this.setAmqPort(61616);
				this.setNumberOfHubs(1);
				this.setNumberOfSpokesPerHub(1);
			}
		};
		
		spawner.execute();
		logger.info("SLEEPING FOR 10 seconds");
		Thread.sleep(10000);
		logger.info("COMPLETED WAIT");
		super.baseSetup("tcp://localhost:61616");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}


	@Test
	public void stompConsumerTest() throws Exception {
		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue("stompQueue");
		ConsumerExecutorManager manager = new ConsumerExecutorManager();
		SimpleStompMessageProcessor processor = new SimpleStompMessageProcessor();
		StompSubscriptionConnectionFactory factory = new StompSubscriptionConnectionFactory("localhost", 61617, "/queue/stompQueue");
		StompConsumerExecutor stompExecutor = new StompConsumerExecutor(factory, processor);
		manager.addExecutor(stompExecutor);
		manager.start();
		ThreadGroup threadGroup = createAndExecuteProducers(session,
				destination);
		// Stop consuming messages....
		manager.stop();
		// consumerExecutor.stopConsumingMessages();
		Thread[] threads = new Thread[DESTINATION_UNITS.length];
		threadGroup.enumerate(threads, false);
		for (Thread thread : threads) {
			if (thread == null)
				return; // No more active threads included in the list, just
						// return
			try {
				logger.info("Waiting for thread: " + thread.getName()
						+ " to exit");
				// Wait until the thread dies prior to exiting test...
				thread.join();
			} catch (InterruptedException ie) {
			}
		}
		logger.info("We are done.");
	}

}
