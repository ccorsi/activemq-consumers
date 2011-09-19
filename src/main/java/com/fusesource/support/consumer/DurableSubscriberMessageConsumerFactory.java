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
import javax.jms.Topic;

/**
 * @author Claudio Corsi
 *
 */
public class DurableSubscriberMessageConsumerFactory extends
		AbstractMessageConsumerFactory {

	private Topic topic;
	private String name;
	
	/**
	 * @param topic
	 */
	public void setTopic(Topic topic) {
		this.topic = topic;
	}
	
	/**
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.MessageConsumerFactory#create()
	 */
	@Override
	public MessageConsumer create() throws JMSException {
		MessageConsumer consumer = null;
		
		if (session == null) {
			throw new JMSException("No session was passed");
		}
		
		if (topic == null) {
			throw new JMSException("No topic was defined");
		}
		
		if (name == null) {
			throw new JMSException("No clientID was defined");
		}
		
		if (messageSelector == null) {
			consumer = session.createDurableSubscriber(topic, name);
		} else {
			consumer = session.createDurableSubscriber(topic, name, messageSelector, noLocal);
		}
		
		return consumer;
	}

}
