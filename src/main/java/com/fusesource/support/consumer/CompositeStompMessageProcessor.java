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
public class CompositeStompMessageProcessor implements StompMessageProcessor {
	
	private static final Logger logger = LoggerFactory.getLogger(CompositeStompMessageProcessor.class);

	private static final FrameConsumer DEFAULT_FRAME_CONSUMER = new FrameConsumer() {

		@Override
		public void onFrame(StompFrame frame) {
			logger.info("Frame body: " + frame.getBody());
			logger.debug("Received stomp frame : " + frame);
		}
		
	};
	
	private volatile boolean consumeMessages = true;

	private FrameConsumer consumer;
	
	public CompositeStompMessageProcessor() {
		this(DEFAULT_FRAME_CONSUMER);
	}
	
	/**
	 * This constructor expects a FrameConsumer instance to be passed.  It will use the
	 * pass frame consumer to process the received StompFrame.
	 * 
	 * @param consumer Instance used to consume StompFrame
	 */
	public CompositeStompMessageProcessor(FrameConsumer consumer) {
		if (consumer == null) {
			throw new IllegalArgumentException("Passed frame consumer is null");
		}
		this.consumer = consumer;
	}
	
	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.StompMessageProcessor#process(org.apache.activemq.transport.stomp.StompConnection)
	 */
	@Override
	public void process(StompConnection connection) throws Exception {
		while(consumeMessages) {
			StompFrame frame = connection.receive();
			logger.info("Processing a stomp frame");
			consumer.onFrame(frame);
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
