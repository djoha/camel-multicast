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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.util.Enumeration;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HelloWorld producer.
 */
public class MulticastProducer extends DefaultProducer {
    private static final transient Logger LOG = LoggerFactory.getLogger(MulticastProducer.class);
    private MulticastEndpoint endpoint;
    private MulticastSocket s;
    
    @Override
    protected void doStart() throws Exception{
    	super.doStart();
    	
    	InetAddress addr = null;
    	Class addrClass;
    	if(endpoint.getMulticastGroup() instanceof Inet4Address){
    		addrClass = Inet4Address.class;
    	}else{
    		addrClass = Inet6Address.class;
    	}
    	
		for(Enumeration<InetAddress> as = endpoint.getInterface().getInetAddresses() ; as.hasMoreElements() ; ){
			InetAddress a = as.nextElement();
			if(addrClass.isInstance(a)){
				addr = a;
			}
		}
		
		if(addr == null){
			addr = endpoint.getInterface().getInetAddresses().nextElement();
		}
		
		LOG.info(String.format("Starting Producer Endpoint on interface %s (%s) ",
				endpoint.getInterface().getDisplayName(),
				addr.getHostAddress()));
    	
		s = new MulticastSocket(new InetSocketAddress(addr.getHostAddress(), endpoint.getSourcePort()));
		s.setReuseAddress(true);
		s.setNetworkInterface(endpoint.getInterface());
		s.setLoopbackMode(true);
//		s.setBroadcast(true);
		
//		s.joinGroup(endpoint.getMulticastGroup());
//		s.connect(endpoint.getMulticastGroup(), endpoint.getAddress().getPort());
//		s.bind(new InetSocketAddress(endpoint.getMulticastGroup(), 0));
    }
    
    @Override
    protected void doStop() throws Exception{
    	super.doStop();
    	s.close();
//    	s.leaveGroup(InetAddress.getByName(endpoint.getAddress().getHost()));
    }
    
    @Override
    protected void doShutdown() throws Exception{
    	super.doShutdown();
    }
    
    
    public MulticastProducer(MulticastEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    public void process(Exchange exchange) throws Exception {   
    	    	
    	InputStream is = exchange.getIn().getBody(InputStream.class);
    	
    	byte[] buf = IOUtils.toByteArray(is);
        DatagramPacket out = new DatagramPacket(buf, buf.length,endpoint.getMulticastGroup(),endpoint.getMulticastAddress().getPort());
        
        try{
        	s.send(out);
            LOG.debug("Packet Sent...");
            StringWriter writer = new StringWriter();
            IOUtils.write(buf, writer);
            LOG.debug(writer.toString());
        }catch(SocketException ex){
        	LOG.error("Socket Closed before DatagramPacket could be sent.",ex);
        }catch(IOException ex){
        	LOG.error("Socket Closed before DatagramPacket could be sent.",ex);
        }

    }

}
