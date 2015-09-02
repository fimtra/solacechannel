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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fimtra.channel.ChannelUtils;
import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageListener;

/**
 * An end-point service based on Solace Systems messaging API
 * 
 * @author Ramon Servadei
 */
public final class SolaceService implements IEndPointService
{
    /**
     * @return the topic for a service end-point
     */
    static Topic createEndPointServiceTopic(String node, int port)
    {
        return SolaceChannelUtils.JCSMP_FACTORY.createTopic("fimtra/" + node + "/" + port);
    }

    /**
     * @return the topic for HB messages sent by a service
     */
    static Topic createServiceHeartbeatTopic(Topic serviceTopic)
    {
        return SolaceChannelUtils.JCSMP_FACTORY.createTopic(serviceTopic.getName() + "/serviceHB");
    }

    /**
     * @return the topic for HB messages sent by a client
     */
    static Topic createClientHeartbeatTopic(Topic serviceTopic)
    {
        return SolaceChannelUtils.JCSMP_FACTORY.createTopic(serviceTopic.getName() + "/clientHB");
    }

    /**
     * A channel implementation that is used only for service heartbeat sending.
     * 
     * @author Ramon Servadei
     */
    private final class SolaceServiceHeartbeatChannel implements ITransportChannel
    {
        SolaceServiceHeartbeatChannel()
        {
        }

        @Override
        public boolean sendAsync(final byte[] toSend)
        {
            if (SolaceService.this.active.get())
            {
                SolaceMultiplexSession.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        doSendAsync(toSend);
                    }
                });
                return true;
            }
            else
            {
                return false;
            }
        }

        void doSendAsync(byte[] toSend)
        {
            final BytesMessage msg = SolaceChannelUtils.getPooledBytesMessage();
            try
            {
                msg.setSenderId(SolaceService.this.serviceId);
                msg.setData(toSend);
                SolaceService.this.session.send(msg, SolaceService.this.serviceHeartbeatTopic);
            }
            finally
            {
                SolaceChannelUtils.returnPooledBytesMessage(msg);
            }
        }

        @Override
        public boolean isConnected()
        {
            return SolaceService.this.active.get();
        }

        @Override
        public boolean hasRxData()
        {
            return SolaceService.this.active.get();
        }

        @Override
        public String getEndPointDescription()
        {
            return ObjectUtils.safeToString(SolaceService.this.serviceHeartbeatTopic);
        }

        @Override
        public String getDescription()
        {
            return getEndPointDescription();
        }

        @Override
        public String toString()
        {
            return "ServiceHeartbeatPublisher [" + getDescription() + "]";
        }

        @Override
        public void destroy(String reason, Exception... e)
        {
        }
    }

    /**
     * An implementation for channels connected to the service
     * 
     * @author Ramon Servadei
     */
    final class SolaceClientChannel implements ITransportChannel
    {
        int rxData;
        final String clientID;
        final Topic clientTopic;
        final IReceiver serviceReceiver;

        SolaceClientChannel(String clientID, IReceiver serviceReceiver)
        {
            this.clientID = clientID;
            this.clientTopic = SolaceChannelUtils.JCSMP_FACTORY.createTopic(clientID);
            this.serviceReceiver = serviceReceiver;
        }

        @Override
        public boolean sendAsync(byte[] toSend)
        {
            if (ChannelUtils.isHeartbeatSignal(toSend))
            {
                // the service sends to a single HB subject, individual client channels do nothing
                return true;
            }

            return SolaceService.this.session.sendAsync(toSend, this.clientTopic, SolaceService.this.serviceTopic);
        }

        @Override
        public boolean isConnected()
        {
            // NOTE: this method is never called by the SolaceService
            return true;
        }

        @Override
        public String getEndPointDescription()
        {
            return ObjectUtils.safeToString(this.clientTopic);
        }

        @Override
        public String getDescription()
        {
            return ObjectUtils.safeToString(SolaceService.this.serviceTopic) + "<->" + getEndPointDescription();
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + " [" + getDescription() + "]";
        }

        @Override
        public void destroy(String reason, Exception... e)
        {
            this.serviceReceiver.onChannelClosed(this);
            SolaceService.this.clients.remove(this.clientID);
        }

        @Override
        public boolean hasRxData()
        {
            final boolean hasRxData = this.rxData > 0;
            this.rxData = 0;
            return hasRxData;
        }
    }

    final String serviceId;
    final Topic serviceTopic;
    final AtomicBoolean active;
    final IReceiver clientReceiver;
    final Topic clientHeartbeatTopic;
    final Topic serviceHeartbeatTopic;
    final SolaceMultiplexSession session;
    final EndPointAddress endPointAddress;
    final XMLMessageListener messageListener;
    final Map<String, Topic> broadcastTopics;
    final Map<String, SolaceClientChannel> clients;
    final ITransportChannel serviceHeartbeatChannel;

    public SolaceService(SolaceMultiplexSession session, EndPointAddress endPointAddress, IReceiver clientReceiver)
    {
        this.serviceId = UUID.randomUUID().toString();
        this.clientReceiver = clientReceiver;
        this.endPointAddress = endPointAddress;
        this.serviceTopic = createEndPointServiceTopic(endPointAddress.getNode(), endPointAddress.getPort());
        this.clientHeartbeatTopic = SolaceService.createClientHeartbeatTopic(this.serviceTopic);
        this.serviceHeartbeatTopic = SolaceService.createServiceHeartbeatTopic(this.serviceTopic);

        this.clients = new ConcurrentHashMap<String, SolaceClientChannel>();
        this.broadcastTopics = new ConcurrentHashMap<String, Topic>();

        this.session = session;
        this.messageListener = new XMLMessageListener()
        {
            @Override
            public void onReceive(BytesXMLMessage message)
            {
                final String clientID = message.getReplyTo().getName();
                SolaceClientChannel client = SolaceService.this.clients.get(clientID);
                if (client == null)
                {
                    client = new SolaceClientChannel(clientID, SolaceService.this.clientReceiver);
                    SolaceService.this.clients.put(clientID, client);
                    ChannelUtils.WATCHDOG.addChannel(client);
                    Log.log(SolaceService.this, "Connected ", ObjectUtils.safeToString(client));
                    SolaceService.this.clientReceiver.onChannelConnected(client);
                }
                client.rxData++;

                final byte[] data = ((BytesMessage) message).getData();
                if (ChannelUtils.isHeartbeatSignal(data))
                {
                    ChannelUtils.WATCHDOG.onHeartbeat(client);
                }
                else
                {
                    SolaceService.this.clientReceiver.onDataReceived(data, client);
                }
            }

            @Override
            public void onException(JCSMPException exception)
            {
                Log.log(SolaceService.this, ObjectUtils.safeToString(SolaceService.this), exception);
            }
        };

        this.active = new AtomicBoolean();
        this.active.set(true);
        this.serviceHeartbeatChannel = new SolaceServiceHeartbeatChannel();
        ChannelUtils.WATCHDOG.addChannel(this.serviceHeartbeatChannel);
        this.session.addSubscription(this.messageListener, this.serviceTopic);
        this.session.addSubscription(this.messageListener, this.clientHeartbeatTopic);

        Log.log(this, "Constructed ", ObjectUtils.safeToString(this), ", serviceId=", this.serviceId);
    }

    @Override
    public EndPointAddress getEndPointAddress()
    {
        return this.endPointAddress;
    }

    @Override
    public void destroy()
    {
        if (this.active.getAndSet(false))
        {
            Log.log(this, "Destroying ", ObjectUtils.safeToString(this));
            this.session.removeSubscription(this.messageListener, this.serviceTopic);
            this.session.removeSubscription(this.messageListener, this.clientHeartbeatTopic);
        }
    }

    @Override
    public int broadcast(final String messageContext, final byte[] txMessage, final ITransportChannel[] clients)
    {
        SolaceMultiplexSession.execute(new Runnable()
        {
            @Override
            public void run()
            {
                doBroadcast(messageContext, txMessage, clients);
            }
        });
        return 1;
    }

    @SuppressWarnings("unused")
    void doBroadcast(String messageContext, byte[] txMessage, ITransportChannel[] clients)
    {
        final BytesMessage msg = SolaceChannelUtils.getPooledBytesMessage();
        try
        {
            msg.setReplyTo(this.serviceTopic);
            msg.setData(txMessage);
            Topic broadcastTopic;
            // todo replace with lock
            synchronized (this.broadcastTopics)
            {
                broadcastTopic = this.broadcastTopics.get(messageContext);
                if (broadcastTopic == null)
                {
                    broadcastTopic =
                        SolaceChannelUtils.JCSMP_FACTORY.createTopic(this.serviceTopic.getName() + "/" + messageContext);
                    Log.log(this, "Creating broadcast topic ", ObjectUtils.safeToString(broadcastTopic));
                    this.broadcastTopics.put(messageContext, broadcastTopic);
                }
            }
            this.session.send(msg, broadcastTopic);
        }
        finally
        {
            SolaceChannelUtils.returnPooledBytesMessage(msg);
        }
    }

    @Override
    public String toString()
    {
        return "SolaceService [" + ObjectUtils.safeToString(this.serviceTopic) + "]";
    }

    @Override
    public void endBroadcast(String messageContext)
    {
        Topic broadcastTopic;
        synchronized (this.broadcastTopics)
        {
            broadcastTopic = this.broadcastTopics.remove(messageContext);
        }
        Log.log(this, "Removed broadcast topic ", ObjectUtils.safeToString(broadcastTopic));
    }
}
