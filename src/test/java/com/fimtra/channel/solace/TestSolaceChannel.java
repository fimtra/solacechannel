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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.channel.StaticEndPointAddressFactory;

/**
 * Tests a {@link SolaceChannel} and {@link SolaceService}
 * 
 * @author Ramon Servadei
 */
public class TestSolaceChannel
{
    IEndPointService solaceEchoService;
    ITransportChannel channel1, channel2;

    @Before
    public void setUp() throws Exception
    {
        ChannelUtils.WATCHDOG.configure(100);
        this.solaceEchoService =
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
    }

    @After
    public void tearDown() throws Exception
    {
        this.solaceEchoService.destroy();
        if (this.channel1 != null)
        {
            this.channel1.destroy("unit test shutdown");
        }
        if (this.channel2 != null)
        {
            this.channel2.destroy("unit test shutdown");
        }
    }

    @Test
    public void testConnectSingleChannelThenShutdownServer() throws Exception
    {
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch messageLatch = new CountDownLatch(1);
        final AtomicReference<String> result = new AtomicReference<String>();
        this.channel1 =
            new SolaceChannelBuilderFactory(new StaticEndPointAddressFactory(new EndPointAddress("node1", 1234))).nextBuilder().buildChannel(
                new IReceiver()
                {
                    @Override
                    public void onDataReceived(byte[] data, ITransportChannel source)
                    {
                        String message = new String(data);
                        System.err.println(source + " received " + message);
                        result.set(message);
                        messageLatch.countDown();
                    }

                    @Override
                    public void onChannelConnected(ITransportChannel channel)
                    {
                        System.err.println("Connected: " + channel.getDescription());
                        connectedLatch.countDown();
                    }

                    @Override
                    public void onChannelClosed(ITransportChannel channel)
                    {
                        System.err.println("Closed: " + channel.getDescription());
                        disconnectedLatch.countDown();
                    }
                });

        assertTrue("Did not get connected signal", connectedLatch.await(5, TimeUnit.SECONDS));

        String message = "hello from client";
        byte[] bytes = new String(message).getBytes();
        this.channel1.sendAsync(bytes);

        assertTrue(messageLatch.await(5, TimeUnit.SECONDS));
        assertEquals(message, result.get());

        this.solaceEchoService.destroy();
        assertTrue("Did not get disconnected signal", disconnectedLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testServiceDetectsClientShutdown() throws Exception
    {
        this.solaceEchoService.destroy();

        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);

        this.solaceEchoService =
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
                    connectedLatch.countDown();
                }

                @Override
                public void onChannelClosed(ITransportChannel channel)
                {
                    System.err.println("Closed: " + channel.getDescription());
                    disconnectedLatch.countDown();
                }
            });

        this.channel1 =
            new SolaceChannelBuilderFactory(new StaticEndPointAddressFactory(new EndPointAddress("node1", 1234))).nextBuilder().buildChannel(
                new IReceiver()
                {
                    @Override
                    public void onDataReceived(byte[] data, ITransportChannel source)
                    {
                    }

                    @Override
                    public void onChannelConnected(ITransportChannel channel)
                    {
                    }

                    @Override
                    public void onChannelClosed(ITransportChannel channel)
                    {
                    }
                });

        assertTrue("Did not get connected signal", connectedLatch.await(5, TimeUnit.SECONDS));

        this.channel1.destroy("unit test shutdown");
        assertTrue("Did not get disconnected signal", disconnectedLatch.await(5, TimeUnit.SECONDS));
        
        Thread.sleep(50);
        
        // check the service removes the channel
        assertEquals(0, ((SolaceService)this.solaceEchoService).clients.size());

    }

    @Test
    public void testBroadcastToMultipleChannels() throws Exception
    {
        final CountDownLatch connectedLatch = new CountDownLatch(2);
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        final CountDownLatch messageLatch = new CountDownLatch(2);
        final AtomicReference<String> result = new AtomicReference<String>();
        this.channel1 =
            new SolaceChannelBuilderFactory(new StaticEndPointAddressFactory(new EndPointAddress("node1", 1234))).nextBuilder().buildChannel(
                new IReceiver()
                {
                    @Override
                    public void onDataReceived(byte[] data, ITransportChannel source)
                    {
                        String message = new String(data);
                        System.err.println(source + " received " + message);
                        result.set(message);
                        messageLatch.countDown();
                    }

                    @Override
                    public void onChannelConnected(ITransportChannel channel)
                    {
                        System.err.println("Connected: " + channel.getDescription());
                        connectedLatch.countDown();
                    }

                    @Override
                    public void onChannelClosed(ITransportChannel channel)
                    {
                        System.err.println("Closed: " + channel.getDescription());
                        disconnectedLatch.countDown();
                    }
                });
        this.channel2 =
            new SolaceChannelBuilderFactory(new StaticEndPointAddressFactory(new EndPointAddress("node1", 1234))).nextBuilder().buildChannel(
                new IReceiver()
                {
                    @Override
                    public void onDataReceived(byte[] data, ITransportChannel source)
                    {
                        String message = new String(data);
                        System.err.println(source + " received " + message);
                        result.set(message);
                        messageLatch.countDown();
                    }

                    @Override
                    public void onChannelConnected(ITransportChannel channel)
                    {
                        System.err.println("Connected: " + channel.getDescription());
                        connectedLatch.countDown();
                    }

                    @Override
                    public void onChannelClosed(ITransportChannel channel)
                    {
                        System.err.println("Closed: " + channel.getDescription());
                        disconnectedLatch.countDown();
                    }
                });

        assertTrue("Did not get connected signal", connectedLatch.await(5, TimeUnit.SECONDS));

        ((SolaceChannel)this.channel1).contextSubscribed("testServerMessage");
        ((SolaceChannel)this.channel2).contextSubscribed("testServerMessage");
        
        String message = "broadcast from server";
        this.solaceEchoService.broadcast("testServerMessage", message.getBytes(), new ITransportChannel[] {this.channel1, this.channel2});

        assertTrue(messageLatch.await(5, TimeUnit.SECONDS));
        assertEquals(message, result.get());

        this.solaceEchoService.destroy();
        assertTrue("Did not get disconnected signal", disconnectedLatch.await(5, TimeUnit.SECONDS));
    }

}
