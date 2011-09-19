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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.brokerservice.runner.HubSpokeBrokerServiceSpawner;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Claudio Corsi
 * 
 */
public class ConsumerExecutorTest extends BaseConsumerExecutorTestSupport {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerExecutorTest.class);
	
	@Before
	public void setup() throws Exception {
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
				return "spoke-activemq";
			}

			{
				logger.info("INSIDE INITIALIZATION");
				this.setAmqPort("61616");
				this.setNumberOfHubs(1);
				this.setNumberOfSpokesPerHub(1);
				port = this.getAmqPort();
				logger.info("port=" + port);
			}
		};
		
		spawner.execute();
		logger.info("SLEEPING FOR 10 seconds");
		Thread.sleep(10000);
		logger.info("COMPLETED WAIT");
		super.baseSetup("tcp://localhost:61616");
	}

	@Test
	public void messageConsumerTest() throws Exception {
		ActiveMQConnectionFactory spokeFactory = new ActiveMQConnectionFactory();
		spokeFactory.setBrokerURL("tcp://localhost:61617");
		Connection spokeConnection = spokeFactory.createConnection();
		spokeConnection.start();
		Session spokeSession = spokeConnection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination spokeDestination = spokeSession.createQueue("simple");
		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue("simple");

		// This part of the code is using the ConsumerExecutor interface....
		ConsumerMessageConsumerFactory messageConsumerFactory = new ConsumerMessageConsumerFactory();
		messageConsumerFactory.setDestination(spokeDestination);
		messageConsumerFactory.setSession(spokeSession);
		SimpleMessageConsumerMessageProcessor messageProcessor = new SimpleMessageConsumerMessageProcessor();
		MessageConsumerExecutor consumerExecutor = new MessageConsumerExecutor(
				messageConsumerFactory, messageProcessor);
		ConsumerExecutorManager manager = new ConsumerExecutorManager();
		manager.addExecutor(consumerExecutor);
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
				System.out.println("Waiting for thread: " + thread.getName()
						+ " to exit");
				// Wait until the thread dies prior to exiting test...
				thread.join();
			} catch (InterruptedException ie) {
			}
		}
		System.out.println("We are done.");
	}


	@Test
	public void compositeMessageConsumerTest() throws Exception {
		ActiveMQConnectionFactory spokeFactory = new ActiveMQConnectionFactory();
		spokeFactory.setBrokerURL("tcp://localhost:61617");
		Connection spokeConnection = spokeFactory.createConnection();
		spokeConnection.start();
		Session spokeSession = spokeConnection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination spokeDestination = spokeSession.createQueue("simple");
		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createQueue("simple");

		// This part of the code is using the ConsumerExecutor interface....
		ConsumerMessageConsumerFactory messageConsumerFactory = new ConsumerMessageConsumerFactory();
		messageConsumerFactory.setDestination(spokeDestination);
		messageConsumerFactory.setSession(spokeSession);
		MessageListenerMessageConsumerMessageProcessor messageProcessor = new MessageListenerMessageConsumerMessageProcessor(
				new MessageListener() {

					@Override
					public void onMessage(Message message) {
						logger.info("Processing message: " + message);
					}

				});
		MessageConsumerExecutor consumerExecutor = new MessageConsumerExecutor(
				messageConsumerFactory, messageProcessor);
		ConsumerExecutorManager manager = new ConsumerExecutorManager();
		manager.addExecutor(consumerExecutor);
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
