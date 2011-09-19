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

import java.net.Socket;
import java.util.HashMap;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;

/**
 * @author Claudio Corsi
 *
 */
public class StompSubscriptionConnectionFactory implements StompConsumerFactory {

	/**
	 * 
	 */
	private static final String DEFAULT_PASSWORD = "manager";
	/**
	 * 
	 */
	private static final String DEFAULT_USERNAME = "system";

	private interface CreateCommand {
		StompConnection create() throws Exception;
		
		void destroy(StompConnection connection) throws Exception;
		
	}
	
	private CreateCommand command;
	
	/**
	 * @param host
	 * @param port
	 * @param destination
	 */
	public StompSubscriptionConnectionFactory(String host, int port, String destination) {
		this(host, port, destination, Stomp.Headers.Subscribe.AckModeValues.AUTO, new HashMap<String, String>(), DEFAULT_USERNAME, DEFAULT_PASSWORD, null);
	}
	
	/**
	 * @param host
	 * @param port
	 * @param destination
	 * @param clientID
	 */
	public StompSubscriptionConnectionFactory(String host, int port, String destination, String clientID) {
		this(host, port, destination, Stomp.Headers.Subscribe.AckModeValues.AUTO, new HashMap<String, String>(), DEFAULT_USERNAME, DEFAULT_PASSWORD, clientID);
	}
	
	/**
	 * @param host
	 * @param port
	 * @param destination
	 * @param username
	 * @param password
	 */
	public StompSubscriptionConnectionFactory(String host, int port, String destination, String username, String password) {
		this(host, port, destination, Stomp.Headers.Subscribe.AckModeValues.AUTO, new HashMap<String, String>(), username, password, null);
	}
	
	/**
	 * @param host
	 * @param port
	 * @param destination
	 * @param username
	 * @param password
	 * @param clientID
	 */
	public StompSubscriptionConnectionFactory(String host, int port, String destination, String username, String password, String clientID) {
		this(host, port, destination, Stomp.Headers.Subscribe.AckModeValues.AUTO, new HashMap<String, String>(), username, password, clientID);
	}
	
	/**
	 * @param host
	 * @param port
	 * @param destination
	 * @param ack
	 * @param headers
	 */
	public StompSubscriptionConnectionFactory(String host, int port, String destination, String ack, HashMap<String, String> headers) {
		this(host, port, destination, ack, headers, DEFAULT_USERNAME, DEFAULT_PASSWORD, null);
	}
	
	/**
	 * @param host
	 * @param port
	 * @param destination
	 * @param ack
	 * @param headers
	 * @param clientID
	 */
	public StompSubscriptionConnectionFactory(String host, int port, String destination, String ack, HashMap<String, String> headers, String clientID) {
		this(host, port, destination, ack, headers, DEFAULT_USERNAME, DEFAULT_PASSWORD, clientID);
	}
	
	/**
	 * @param host
	 * @param port
	 * @param destination
	 * @param ack
	 * @param headers
	 * @param username
	 * @param password
	 * @param clientID
	 */
	public StompSubscriptionConnectionFactory(String host, int port, String destination, String ack, HashMap<String, String> headers, String username, String password, String clientID) {
		this.command = new CreateCommand() {

			private String host;
			private int port;
			private String destination;
			private HashMap<String, String> headers;
			private String ack;
			private String password;
			private String username;
			private String clientID;

			@Override
			public StompConnection create() throws Exception {
				Socket socket = new Socket(host, port);
				StompConnection connection = new StompConnection();
				connection.setStompSocket(socket);
				if (clientID == null) {
					connection.connect(username, password);
				} else {
					connection.connect(username, password, clientID);
				}
				connection.subscribe(destination, ack, headers);
				return connection;
			}


			@Override
			public void destroy(StompConnection connection) throws Exception {
				connection.unsubscribe(destination);
				connection.close();
			}
			
			public CreateCommand setFields(String host, int port, String destination, String ack, HashMap<String, String> headers, String username, String password, String clientID) {
				this.host = host;
				this.port = port;
				this.destination = destination;
				this.ack = ack;
				this.headers = headers;
				this.username = username;
				this.password = password;
				this.clientID = clientID;
				return this;
			}
			
		}.setFields(host, port, destination, ack, headers, username, password, clientID);
	}
	
	public StompSubscriptionConnectionFactory(Socket socket, String destination, String ack, HashMap<String,String> headers, String username, String password, String clientID) {
		this.command = new CreateCommand() {

			private Socket socket;
			private String destination;
			private String ack;
			private HashMap<String, String> headers;
			private String username;
			private String password;
			private String clientID;

			@Override
			public StompConnection create() throws Exception {
				StompConnection connection = new StompConnection();
				connection.setStompSocket(socket);
				if (clientID == null) {
					connection.connect(username, password);
				} else {
					connection.connect(username, password, clientID);
				}
				connection.subscribe(destination, ack, headers);
				return connection;
			}

			@Override
			public void destroy(StompConnection connection) throws Exception {
				connection.unsubscribe(destination);
				connection.close();
			}
			
			public CreateCommand setFields(Socket socket, String destination, String ack, HashMap<String, String> headers, String username, String password, String clientID) {
				this.socket = socket;
				this.destination = destination;
				this.ack = ack;
				this.headers = headers;
				this.username = username;
				this.password = password;
				this.clientID = clientID;
				return this;
			}

		}.setFields(socket, destination, ack, headers, username, password, clientID);
	}
	
	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.StompConsumerFactory#create()
	 */
	@Override
	public StompConnection create() throws Exception {
		return this.command.create();
	}

	/* (non-Javadoc)
	 * @see com.fusesource.support.consumer.StompConsumerFactory#destroy(org.apache.activemq.transport.stomp.StompConnection)
	 */
	@Override
	public void destroy(StompConnection connection) throws Exception {
		this.command.destroy(connection);
	}

}
