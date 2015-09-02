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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ISubscribingChannel;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageListener;

/**
 * A transport channel based on Solace Systems messaging API
 * 
 * @author Ramon Servadei
 */
public final class SolaceChannel implements ITransportChannel, ISubscribingChannel
{
    int rxData;
    String serverId;
    final IReceiver receiver;
    /** The topic the service listens to for messages from clients */
    final Topic serviceTopic;
    /** The unique topic for replies to be sent from the service to the client */
    final Topic replyTopic;
    /** The topic for receiving the HB signals from the service */
    final Topic serviceHeartbeatTopic;
    /** The topic a client uses for sending HB signals to the service */
    final Topic clientHeartbeatTopic;
    final SolaceMultiplexSession session;
    /**
     * Tracks if the {@link IReceiver#onChannelConnected(ITcpChannel)} has been called. Ensures its
     * only called once.
     */
    final AtomicBoolean onChannelConnectedCalled;
    /**
     * Tracks if the {@link IReceiver#onChannelClosed(ITcpChannel)} has been called. Ensures its
     * only called once.
     */
    final AtomicBoolean onChannelClosedCalled;
    final XMLMessageListener listener;

    public SolaceChannel(SolaceMultiplexSession session, Topic serviceTopic, IReceiver receiver)
    {
        this.receiver = receiver;
        this.serviceTopic = serviceTopic;
        this.clientHeartbeatTopic = SolaceService.createClientHeartbeatTopic(serviceTopic);
        this.serviceHeartbeatTopic = SolaceService.createServiceHeartbeatTopic(serviceTopic);
        this.session = session;
        this.onChannelConnectedCalled = new AtomicBoolean();
        this.onChannelClosedCalled = new AtomicBoolean();

        this.replyTopic = SolaceChannelUtils.JCSMP_FACTORY.createTopic("fimtra/" + UUID.randomUUID().toString());
        this.listener = new XMLMessageListener()
        {
            @Override
            public void onReceive(BytesXMLMessage message)
            {
                SolaceChannel.this.rxData++;
                final byte[] data = ((BytesMessage) message).getData();
                if (ChannelUtils.isHeartbeatSignal(data))
                {
                    if (SolaceChannel.this.serverId == null)
                    {
                        SolaceChannel.this.serverId = message.getSenderId();
                    }
                    else
                    {
                        if (!SolaceChannel.this.serverId.equals(message.getSenderId()))
                        {
                            destroy("Service ID changed from [" + SolaceChannel.this.serverId + "] to ["
                                + message.getSenderId() + "]");
                        }
                    }
                    ChannelUtils.WATCHDOG.onHeartbeat(SolaceChannel.this);
                    if (!SolaceChannel.this.onChannelConnectedCalled.getAndSet(true))
                    {
                        Log.log(this, "Connected ", ObjectUtils.safeToString(SolaceChannel.this));
                        SolaceChannel.this.receiver.onChannelConnected(SolaceChannel.this);
                    }
                }
                else
                {
                    SolaceChannel.this.receiver.onDataReceived(data, SolaceChannel.this);
                }
            }

            @Override
            public void onException(JCSMPException exception)
            {
                Log.log(SolaceChannel.this, ObjectUtils.safeToString(SolaceChannel.this), exception);
            }
        };

        ChannelUtils.WATCHDOG.addChannel(this);

        this.session.addSubscription(this.listener, this.serviceHeartbeatTopic);
        this.session.addSubscription(this.listener, this.replyTopic);
    }

    @Override
    public void destroy(String reason, Exception... e)
    {
        if (this.onChannelClosedCalled.getAndSet(true))
        {
            return;
        }

        if (e == null || e.length == 0)
        {
            Log.log(this, reason, " ", ObjectUtils.safeToString(this));
        }
        else
        {
            Log.log(this, reason + " " + this, e[0]);
        }

        try
        {
            this.session.removeSubscription(this.listener, this.replyTopic);
            this.session.removeSubscription(this.listener, this.serviceHeartbeatTopic);
            this.receiver.onChannelClosed(this);
        }
        catch (Exception e1)
        {
            Log.log(this, "Could not destroy " + this, e1);
        }
    }

    @Override
    public void contextSubscribed(String subscriptionContext)
    {
        this.session.addSubscription(this.listener, createRecordSubscriptionTopic(subscriptionContext));
    }

    @Override
    public void contextUnsubscribed(String subscriptionContext)
    {
        this.session.removeSubscription(this.listener, createRecordSubscriptionTopic(subscriptionContext));
    }

    @Override
    public boolean sendAsync(byte[] toSend)
    {
        if (ChannelUtils.isHeartbeatSignal(toSend))
        {
            return this.session.sendAsync(toSend, this.clientHeartbeatTopic, this.replyTopic);
        }
        return this.session.sendAsync(toSend, this.serviceTopic, this.replyTopic);
    }

    @Override
    public boolean isConnected()
    {
        return this.onChannelConnectedCalled.get() && !this.onChannelClosedCalled.get();
    }

    @Override
    public String getEndPointDescription()
    {
        return ObjectUtils.safeToString(this.serviceTopic);
    }

    @Override
    public String getDescription()
    {
        return ObjectUtils.safeToString(this.replyTopic) + "<->" + ObjectUtils.safeToString(this.serviceTopic);
    }

    @Override
    public String toString()
    {
        return "SolaceChannel [" + getDescription() + "]";
    }

    private Topic createRecordSubscriptionTopic(String subscriptionContext)
    {
        return SolaceChannelUtils.JCSMP_FACTORY.createTopic(this.serviceTopic.getName() + "/" + subscriptionContext);
    }

    @Override
    public boolean hasRxData()
    {
        final boolean hasRxData = this.rxData > 0;
        this.rxData = 0;
        return hasRxData;
    }
}
