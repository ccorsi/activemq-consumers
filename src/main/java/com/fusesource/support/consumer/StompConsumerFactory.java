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
public interface StompConsumerFactory {

	/**
	 * This method is called to create an StompConnection instance
	 * 
	 * @return An instance of a StompConnection
	 * @throws Exception 
	 */
	StompConnection create() throws Exception;
	
	/**
	 * This method is called whenever the connection needs to be released.
	 * 
	 * @param connection  The connection that will be destroy
	 * @throws Exception Thrown while trying to release the passed stomp connection
	 */
	void destroy(StompConnection connection) throws Exception;
	
}
