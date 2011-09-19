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

import java.util.Random;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.brokerservice.runner.AbstractBrokerServiceSpawner;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fusesource.support.message.chain.IntegerMessageChain;
import com.fusesource.support.message.chain.MessageChain;
import com.fusesource.support.message.chain.StringMessageChain;
import com.fusesource.support.message.creator.MessageCreator;
import com.fusesource.support.message.creator.TextMessageCreator;
import com.fusesource.support.message.sender.SimpleMessageSender;
import com.fusesource.support.producer.MessageProducerFactory;
import com.fusesource.support.producer.ProducerExecutor;

/**
 * @author Claudio Corsi
 *
 */
public class BaseConsumerExecutorTestSupport {

	private static final Logger logger = LoggerFactory.getLogger(BaseConsumerExecutorTestSupport.class);
	
	protected static final int DESTINATION_UNITS[] = { 3039, 3139, 3239, 3339,
				3439, 3539, };
	protected AbstractBrokerServiceSpawner spawner;
	protected Connection connection;
	private ActiveMQConnectionFactory factory;

	/**
	 * This method should be called by the sub-class setup method because the broker needs to 
	 * be started prior to this being called.
	 * 
	 * Not calling this can cause issues with the executing test....
	 */
	public void baseSetup(String clientURI) throws JMSException {
		// We have to wait for the activemq broker to start.
		factory = new ActiveMQConnectionFactory();
		factory.setBrokerURL(clientURI);
		connection = factory.createConnection();
		connection.start();
	}
	
	/**
	 * @param session
	 * @param destination
	 * @return The thread group of created producer threads
	 * @throws JMSException
	 * @throws InterruptedException
	 */
	protected ThreadGroup createAndExecuteProducers(Session session, Destination destination)
			throws JMSException, InterruptedException {
				ThreadGroup threadGroup = new ThreadGroup("ProducerExecuterThreadGroup");
				ProducerExecutor producerExecutors[] = new ProducerExecutor[DESTINATION_UNITS.length];
				int idx = 0;
				for (int destinationUnit : DESTINATION_UNITS) {
					MessageCreator messageCreator = new TextMessageCreator(session);
					messageCreator.add(new IntegerMessageChain("DestinationUnit",
							destinationUnit));
					messageCreator.add(new StringMessageChain("DestinationDept", "DC"));
					messageCreator.add(new StringMessageChain("OriginDept", "ISMerch"));
					messageCreator.add(new IntegerMessageChain("OriginUnit", 1001));
					messageCreator.add(new StringMessageChain("MessageType", "PO"));
					if (idx % 2 == 0) {
						messageCreator.add(new MessageChain() {
			
							private Random rand = new Random();
			
							@Override
							public void apply(Message message) throws JMSException {
								try {
									long millis = rand.nextInt(5000) + 1;
									logger.info("Sleeping for " + millis + "ms.");
									Thread.sleep(millis);
								} catch (InterruptedException ie) {
									// Do nothing....
								}
							}
			
						});
					}
			
					producerExecutors[idx] = new ProducerExecutor(
							new MessageProducerFactory(session, destination).create(),
							messageCreator, new SimpleMessageSender());
			
					new Thread(threadGroup, new Runnable() {
			
						private ProducerExecutor producerExecutor;
			
						public void run() {
							producerExecutor.execute();
						}
			
						public Runnable setProducerExecutor(
								ProducerExecutor producerExecutor) {
							this.producerExecutor = producerExecutor;
							return this;
						}
			
					}.setProducerExecutor(producerExecutors[idx++])) {
						{
							setDaemon(true);
							start();
						}
					};
				}
			
				try {
					logger.info("WAITING FOR 10 seconds prior to sending a message");
					Thread.sleep(10000);
				} finally {
					for (ProducerExecutor producerExecutor : producerExecutors) {
						producerExecutor.stopCreatingMessages();
					}
					logger.info("STOP SENDING MESSAGES");
				}
				return threadGroup;
			}

	@After
	public void teardown() throws Exception {
		if (spawner != null)
			spawner.stopBrokers();
		if (connection != null)
			connection.close();
	}

}
