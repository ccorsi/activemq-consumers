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

/**
 * 
 * This interface will enable us to create the following types of MessageConsumers
 * 
 * 	- durable subscriptions (topic and queue?)
 *  - subscriptions (topic and queue?)
 *  - topic
 *  - queue
 *  
 * @author Claudio Corsi
 *
 */
public interface MessageConsumerFactory {

	MessageConsumer create() throws JMSException;
	
}
