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

import javax.jms.JMSException;
import javax.jms.MessageConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Claudio Corsi
 *
 */
public class MessageConsumerExecutor implements ConsumerExecutor {
	
	private static final Logger logger = LoggerFactory.getLogger(MessageConsumerExecutor.class);
	
	private MessageConsumerFactory messageConsumerFactory;
	private MessageProcessor processor;
	private MessageConsumer consumer;

	public MessageConsumerExecutor(MessageConsumerFactory messageConsumerFactory, MessageProcessor processor) {
		this.messageConsumerFactory = messageConsumerFactory;
		this.processor = processor;
		logger.info("Created a message consumer executor with " + messageConsumerFactory + " and " + processor);
	}
	
	public void init() throws Exception {
		logger.info("Creating MessageConsumer");
		this.consumer = this.messageConsumerFactory.create();
		logger.info("Created MessageConsumer");
	}
	
	public void execute() throws JMSException {
		try {
			logger.info("Executing message processor");
			while (processor.consumeMoreMessages()) {
				processor.processMessages(consumer);
			}
		} finally {
			// Cleanup the consumer connection....
			// this.consumer.close();
		}
		logger.info("Completed execute method");
	}

	public void stopConsumingMessages() throws JMSException {
		logger.info("Calling stop consuming messages for the message processor");
		processor.stopConsumingMessages();
	}
	
}
