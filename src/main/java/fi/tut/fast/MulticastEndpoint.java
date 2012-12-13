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
package fi.tut.fast;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.component.direct.DirectEndpoint;
import org.apache.camel.impl.DefaultEndpoint;

/**
 * Represents a HelloWorld endpoint.
 */
public class MulticastEndpoint extends DirectEndpoint {
		
	private int socketTimeout = 10000;
	private String networkInterface = null;
	private int maxPacketSize = 1024;
	private URI mcAddress;			// Multicast Address
	private NetworkInterface iface;
	private InetAddress address;	// Address on Network Interface;
	private int sourcePort = 0;
	
	
	public URI getMulticastAddress() {
		return mcAddress;
	}
	
	public NetworkInterface getInterface() throws SocketException, UnknownHostException{
		
		if(iface != null){
			return iface;
		}
		
		if(getNetworkInterface() == null){
			iface =  NetworkInterface.getByInetAddress(getMulticastGroup());
			if(iface == null){
				iface = NetworkInterface.getNetworkInterfaces().nextElement();
			}
		}else{
			iface = NetworkInterface.getByName(getNetworkInterface());
		}
		return iface;
	}
	
	public InetAddress getAddress() throws SocketException, UnknownHostException{
    	if(address != null){
    		return address;
    	}
    	Class addrClass;
    	if(getMulticastGroup() instanceof Inet4Address){
    		addrClass = Inet4Address.class;
    	}else{
    		addrClass = Inet6Address.class;
    	}
    	
		for(Enumeration<InetAddress> as = getInterface().getInetAddresses() ; as.hasMoreElements() ; ){
			InetAddress a = as.nextElement();
			if(addrClass.isInstance(a)){
				address = a;
			}
		}
		
		if(address == null){
			getInterface().getInetAddresses().nextElement();
		}
		
		return address;
	}
	
	public int getPort(){
		return getMulticastAddress().getPort();
	}
	
	public InetAddress getMulticastGroup() throws UnknownHostException{
		return InetAddress.getByName(getMulticastAddress().getHost());
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public String getNetworkInterface() {
		return networkInterface;
	}

	public void setNetworkInterface(String networkInterface) {
		this.networkInterface = networkInterface;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public MulticastEndpoint() {
		
	}

	public MulticastEndpoint(String uri, MulticastComponent component) throws URISyntaxException {
		super(uri, component);
		mcAddress =  new URI(uri);
	}

	public MulticastEndpoint(String endpointUri) {
		super(endpointUri);
	}

	public Producer createProducer() throws Exception {
		hasproducer = true;
		return new MulticastProducer(this);
	}
	
	boolean hasconsumer = false;
	boolean hasproducer = false;

	public Consumer createConsumer(Processor processor) throws Exception {
		hasconsumer = true;
		return new MulticastConsumer(this, processor);
	}

	public boolean isSingleton() {
		return true;
	}

	public int getSourcePort() {
		return sourcePort;
	}

	public void setSourcePort(int sourcePort) {
		this.sourcePort = sourcePort;
	}
}
