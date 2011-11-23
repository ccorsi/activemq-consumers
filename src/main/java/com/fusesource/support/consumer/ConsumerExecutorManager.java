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

import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will manage the starting and stopping of the ConsumerExecutor instances. 
 * It will follow the expected protocol for starting/executing and stopping the different
 * instance of the ConsumerExecutor instances.
 * 
 * @author Claudio Corsi
 *
 */
public class ConsumerExecutorManager {
	
	private static final Logger logger = LoggerFactory.getLogger(ConsumerExecutorManager.class);
	
	private Set<ConsumerExecutor> executors = new HashSet<ConsumerExecutor>();
	private boolean started = false;
	
	public void addExecutor(ConsumerExecutor executor) {
		synchronized(executors) {
			executors.add(executor);
			if (started) {
				// Create a thread since this manager has already started the consumers....
				createConsumerExecutorThread(executor);
			}
		}
	}
	
	public void start() throws Exception {
		// This method is used to start all of the added ConsumerExecutors...
		synchronized(executors) {
			if(started) return;
			
			for(ConsumerExecutor executor : executors) {
				// Create a Thread and execute the executor....
				createConsumerExecutorThread(executor);
			}
			started = true;
		}
	}

	/**
	 * @param executor
	 */
	private void createConsumerExecutorThread(ConsumerExecutor executor) {
		new Thread(new Runnable() {

			private ConsumerExecutor consumerExecutor;

			/*
			 * (non-Javadoc)
			 * 
			 * @see java.lang.Runnable#run()
			 */
			public void run() {
				try {
					// Call init method....
					consumerExecutor.init();
					// Call execute method...
					consumerExecutor.execute();
				} catch (Exception e) {
					logger.debug("Received an exception while executing consumer message executor", e);
				} finally {
					try {
						// TODO: Should this be called here or should this be
						// explicit?
						consumerExecutor.stopConsumingMessages();
					} catch (JMSException e) {
						logger.debug("Received an exception while calling stop consumer messages for the consumer executor", e);
					}
				}
			}

			public Runnable setConsumerExecutor(
					ConsumerExecutor consumerExecutor) {
				this.consumerExecutor = consumerExecutor;
				return this;
			}

		}.setConsumerExecutor(executor)) {
			{
				this.setDaemon(true);
				this.start();
			}
		};
	}
	
	public void stop() {
		// This method is used to stop all of the active ConsumerExecutors.
		synchronized(executors) {
			if(!started) return;
			
			for(ConsumerExecutor executor : executors) {
				// Calling the stopping method...
				try {
					executor.stopConsumingMessages();
				} catch (JMSException e) {
					logger.debug("An exception was raised when trying to stop the executor", e);
				}
			}
			
			started = false;
		}
	}
	
	public boolean remoteExecutor(ConsumerExecutor executor) {
		boolean result = executors.remove(executor);
		try {
			executor.stopConsumingMessages();
		} catch (JMSException e) {
			logger.debug("An exception was raised when trying to stop consumers", e);
		}
		return result;
	}

}
