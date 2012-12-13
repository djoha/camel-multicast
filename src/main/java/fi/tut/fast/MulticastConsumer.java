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
import java.io.StringWriter;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.direct.DirectConsumer;
import org.apache.camel.converter.stream.InputStreamCache;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * The HelloWorld consumer.
 */
public class MulticastConsumer extends DirectConsumer {
	
    private static final transient Logger LOG = LoggerFactory.getLogger(MulticastProducer.class);

    private final MulticastEndpoint endpoint;
	private MulticastSocket s;

	private Queue<Runnable> messageQueue;
	private Thread dlThread;
	private boolean done = false;
	
    public MulticastConsumer(MulticastEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }
    
    @Override
    protected void doStart() throws Exception{
    	super.doStart();

//    	System.out.println(endpoint.getEndpointUri());
//		System.out.println("Starting Comsumer Endpoint on interface " + endpoint.getInterface().getDisplayName());
    	
    	LOG.info(String.format("Starting Comsumer Endpoint on interface %s" ,
    			endpoint.getInterface().getDisplayName()));
    	
    	
		messageQueue = new MessageQueue();
		s = new MulticastSocket(endpoint.getMulticastAddress().getPort());
		s.setReuseAddress(true);
		s.setNetworkInterface(endpoint.getInterface());
		s.joinGroup(InetAddress.getByName(endpoint.getMulticastAddress().getHost()));
		s.setSoTimeout(endpoint.getSocketTimeout());
		
		dlThread = new Thread(new Runnable(){

			@Override
			public void run() {
				while(!done){
					try {
						consume();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				try {
					s.leaveGroup(InetAddress.getByName(endpoint.getMulticastAddress().getHost()));
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				s.close();
				
			}
			
		});
		dlThread.setDaemon(true);
		dlThread.start();
		
    }
    
    @Override
    protected void doStop() throws Exception{
    	done = true;
    	super.doStop();
    }
    
    @Override
	public void prepareShutdown(){
    	done = true;
    	super.prepareShutdown() ;
    }
    
    @Override
    public int getPendingExchangesSize() {
    	return messageQueue.size();
    }
    
    
	private void consume() throws IOException{
		byte buf[] = new byte[endpoint.getMaxPacketSize()];
		DatagramPacket pack = new DatagramPacket(buf, buf.length);
		try{
			s.receive(pack);
			try {
				messageQueue.add(new ExchangeDeliveryJob(pack.getData(),pack.getLength()));
			} catch (Exception e) {
				LOG.error("Exception Caught while processing UDP message.", e);
			}
		}catch(java.net.SocketTimeoutException ex){
			// Timed out.. Listen again.
		}
	}
    
	class ExchangeDeliveryJob implements Runnable{
		
		byte buf[];
		int length;
		
		public ExchangeDeliveryJob(byte[] buf, int len){
			this.buf = Arrays.copyOf(buf,len);
			this.length = len;
		}

		@Override
		public void run() {
			Exchange exchange = endpoint.createExchange();
//			exchange.getIn().setBody(new InputStreamCache(buf));
			exchange.getIn().setBody(buf);
//
////            StringWriter writer = new StringWriter();
//            try {
////				IOUtils.write(buf, writer);
//			} catch (IOException e1) {
//				LOG.error("Exception Caught while processing UDP message.", e1);
//			}
            try {
				getProcessor().process(exchange);
			} catch (Exception e) {
	            if (exchange.getException() != null) {
	                getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
	            }
			}
		}
		
	}
	
class MessageQueue extends ConcurrentLinkedQueue<Runnable>{
		
		Thread processor;
		private final Object lock = new Object();
		public MessageQueue(){
			processor = new Thread(new Runnable(){
				@Override
				public void run() {
					while(true){
						try {
							synchronized(lock){
								lock.wait();
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						while(!isEmpty()){
							poll().run();
						}
					}
				}
			});
			processor.start();
		}
		
		@Override
		public boolean offer(Runnable job){
			boolean result = super.offer(job);
			synchronized(lock){
				lock.notifyAll();
			}
			return result;
		}
		
	}
    
}
