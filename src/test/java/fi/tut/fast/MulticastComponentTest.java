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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

public class MulticastComponentTest extends CamelTestSupport {

    @Test
    public void testTimerInvokesBeanMethod() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedMinimumMessageCount(1);       

        try{
            URL siteUrl = new URL("http://192.168.3.79/config/system.html");
            HttpURLConnection conn = (HttpURLConnection) siteUrl.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            DataOutputStream out = new DataOutputStream(conn.getOutputStream());
            out.writeBytes("reboot=reboot&__end=");
            out.flush();
            out.close();
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String line;
			while((line=in.readLine())!=null) {
				System.out.println(line);
			}
			in.close();
        }catch(Exception ex){
            System.err.println("Restart Attempt Failed.");
            ex.printStackTrace();
        }
        
        
        assertMockEndpointsSatisfied();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() {
                from("multicast://239.255.255.250:3702?networkInterface=en0")    // will send a message every 500ms
                  .to("multicast://239.255.255.250:3702?socketTimeout=500&maxPacketSize=1024&networkInterface=en0")   // prints message to stdout
                  .to("mock:result");       // to actually test that a message arrives
            }
        };
    }
}
