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
import org.apache.activemq.transport.stomp.StompFrame;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Claudio Corsi
 *
 */
public class SimpleStompMessageProcessor implements StompMessageProcessor {
	
	private static final Logger logger = LoggerFactory.getLogger(SimpleStompMessageProcessor.class);
	
	private volatile boolean consumeMessages = true;
	
	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.StompMessageProcessor#process(org.apache.activemq.transport.stomp.StompConnection)
	 */
	@Override
	public void process(StompConnection connection) throws Exception {
		while(consumeMessages) {
			StompFrame frame = connection.receive();
			logger.info("Frame body: " + frame.getBody());
			logger.debug("Received stomp frame : " + frame);
		}
	}

	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.StompMessageProcessor#stopConsumingMessages()
	 */
	@Override
	public void stopConsumingMessages() {
		consumeMessages = false;
	}

	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.StompMessageProcessor#consumeMoreMessages()
	 */
	@Override
	public boolean consumeMoreMessages() {
		return consumeMessages;
	}

}
