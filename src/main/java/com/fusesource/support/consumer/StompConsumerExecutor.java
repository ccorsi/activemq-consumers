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

import org.apache.activemq.transport.stomp.StompConnection;

/**
 * @author Claudio Corsi
 *
 */
public class StompConsumerExecutor implements ConsumerExecutor {

	private volatile boolean consumeMessages;

	private StompConsumerFactory consumerFactory;
	private StompMessageProcessor processor;
	private StompConnection connection;

	public StompConsumerExecutor(StompConsumerFactory consumerFactory, StompMessageProcessor processor) {
		this.consumerFactory = consumerFactory;
		this.processor = processor;
	}
	
	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.ConsumerExecutor#execute()
	 */
	@Override
	public void execute() throws Exception {
		try {
			while (consumeMessages && processor.consumeMoreMessages()) {
				processor.process(connection);
			}
		} finally {
			processor.stopConsumingMessages();
			consumeMessages = false;
		}
	}

	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.ConsumerExecutor#init()
	 */
	@Override
	public void init() throws Exception {
		connection = consumerFactory.create();
		consumeMessages = true;
	}

	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.ConsumerExecutor#stopConsumingMessages()
	 */
	@Override
	public void stopConsumingMessages() {
		processor.stopConsumingMessages();
		consumeMessages = false;
	}

}
