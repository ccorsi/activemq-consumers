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
import javax.jms.MessageListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Claudio Corsi
 *
 */
public class MessageListenerMessageConsumerMessageProcessor implements MessageProcessor {

	private static final Logger logger = LoggerFactory.getLogger(MessageListenerMessageConsumerMessageProcessor.class);
	private MessageListener listener;
	private MessageConsumer consumer;
	private volatile boolean moreMessages = true;

	public MessageListenerMessageConsumerMessageProcessor(MessageListener listener) {
		if (listener == null) {
			throw new IllegalArgumentException("MessageListener can not be null");
		}
		this.listener = listener;
	}
	
	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.MessageProcessor#processMessages(javax.jms.MessageConsumer)
	 */
	@Override
	public void processMessages(MessageConsumer consumer) throws JMSException {
		if (this.consumer == null) {
			this.consumer = consumer;
			logger.debug("Setting message listener to: " + listener);
			consumer.setMessageListener(listener);
			logger.info("Set message listener");
		}
	}

	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.MessageProcessor#stopConsumingMessages()
	 */
	@Override
	public void stopConsumingMessages() throws JMSException {
		logger.info("Indicating to stop processing messages");
		this.moreMessages = false;
	}

	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.MessageProcessor#consumeMoreMessages()
	 */
	@Override
	public boolean consumeMoreMessages() {
		return moreMessages;
	}

}
