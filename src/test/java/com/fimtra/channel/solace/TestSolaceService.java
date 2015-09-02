/*
 * Copyright (c) 2015 Ramon Servadei 
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.channel.solace;

import java.io.IOException;

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.channel.StaticEndPointAddressFactory;

/**
 * @author Ramon Servadei
 */
public class TestSolaceService
{
    public static void main(String[] args) throws Exception
    {
        new TestSolaceService().testSolaceService();
    }

    @SuppressWarnings("unused")
    public void testSolaceService() throws IOException
    {
        IEndPointService solaceService =
            new SolaceServiceBuilder(new EndPointAddress("node1", 1234)).buildService(new IReceiver()
            {

                @Override
                public void onDataReceived(byte[] data, ITransportChannel source)
                {
                    System.err.println("Service received from " + source + ", message=" + new String(data));
                    source.sendAsync(data);
                }

                @Override
                public void onChannelConnected(ITransportChannel channel)
                {
                    System.err.println("Connected: " + channel.getDescription());
                }

                @Override
                public void onChannelClosed(ITransportChannel channel)
                {
                    System.err.println("Closed: " + channel.getDescription());
                }
            });

        ITransportChannel channel =
            new SolaceChannelBuilderFactory(new StaticEndPointAddressFactory(new EndPointAddress("node1", 1234))).nextBuilder().buildChannel(
                new IReceiver()
                {

                    @Override
                    public void onDataReceived(byte[] data, ITransportChannel source)
                    {
                        System.err.println(source + " recieved " + new String(data));
                    }

                    @Override
                    public void onChannelConnected(ITransportChannel channel)
                    {
                        System.err.println("Connected: " + channel.getDescription());
                    }

                    @Override
                    public void onChannelClosed(ITransportChannel channel)
                    {
                        System.err.println("Closed: " + channel.getDescription());
                    }
                });

        byte[] bytes = new String("hello from client").getBytes();

        channel.sendAsync(bytes);
        System.in.read();
    }
}
