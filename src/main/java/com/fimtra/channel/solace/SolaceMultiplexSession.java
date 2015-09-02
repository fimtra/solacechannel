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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.fimtra.util.Log;
import com.fimtra.util.ObjectUtils;
import com.fimtra.util.SubscriptionManager;
import com.fimtra.util.ThreadUtils;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

/**
 * Provides the capability for a single Solace {@link JCSMPSession} to be used by multiple
 * publishers/subscribers.
 * 
 * @author Ramon Servadei
 */
public final class SolaceMultiplexSession
{
    static final Executor txThread =
        ThreadUtils.newSingleThreadExecutorService(SolaceMultiplexSession.class.getSimpleName() + "-tx");

    static void execute(Runnable runnable)
    {
        txThread.execute(runnable);
    }

    final JCSMPSession session;
    final XMLMessageProducer solaceTx;
    final XMLMessageConsumer solaceRx;
    final SubscriptionManager<String, XMLMessageListener> serviceTopicListeners;

    SolaceMultiplexSession()
    {
        super();
        final JCSMPProperties properties = new JCSMPProperties();
        try
        {
            Properties p = new Properties();
            p.load(Object.class.getResourceAsStream("/solace-channel.properties"));

            Map.Entry<Object, Object> entry = null;
            Object key = null;
            Object value = null;
            for (Iterator<Map.Entry<Object, Object>> it = p.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                properties.setProperty(key.toString(), value);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not load solace settings from solace-channel.properties file", e);
        }
        try
        {
            this.session = JCSMPFactory.onlyInstance().createSession(properties);
        }
        catch (InvalidPropertiesException e)
        {
            throw new RuntimeException("Could not create session from " + properties, e);
        }

        this.serviceTopicListeners = new SubscriptionManager<String, XMLMessageListener>(XMLMessageListener.class);
        try
        {
            this.solaceTx = this.session.getMessageProducer(new JCSMPStreamingPublishEventHandler()
            {
                @Override
                public void responseReceived(String messageID)
                {
                    // noop - only direct messaging is used so this callback is never invoked
                }

                @Override
                public void handleError(String messageID, JCSMPException e, long timestamp)
                {
                    Log.log(SolaceMultiplexSession.this, "Error received: " + messageID, e);
                }
            });
            this.solaceRx = this.session.getMessageConsumer(new XMLMessageListener()
            {
                @Override
                public void onReceive(BytesXMLMessage message)
                {
                    final XMLMessageListener[] subscribersFor =
                        SolaceMultiplexSession.this.serviceTopicListeners.getSubscribersFor(message.getDestination().getName());
                    for (int i = 0; i < subscribersFor.length; i++)
                    {
                        try
                        {
                            subscribersFor[i].onReceive(message);
                        }
                        catch (Exception e)
                        {
                            Log.log(SolaceMultiplexSession.this,
                                "Could not notify " + ObjectUtils.safeToString(subscribersFor[i]) + " with "
                                    + ObjectUtils.safeToString(message), e);
                        }
                    }
                }

                @Override
                public void onException(JCSMPException exception)
                {
                    Log.log(SolaceMultiplexSession.this, ObjectUtils.safeToString(SolaceMultiplexSession.this),
                        exception);
                }
            });
            this.solaceRx.start();
        }
        catch (JCSMPException e)
        {
            throw new RuntimeException("Could not create multiplex session using "
                + ObjectUtils.safeToString(this.session), e);
        }
    }

    boolean sendAsync(final byte[] toSend, final Topic destination, final Topic reply)
    {
        execute(new Runnable()
        {
            @Override
            public void run()
            {
                final BytesMessage msg = SolaceChannelUtils.getPooledBytesMessage();
                try
                {
                    msg.setReplyTo(reply);
                    msg.setData(toSend);
                    send(msg, destination);
                }
                finally
                {
                    SolaceChannelUtils.returnPooledBytesMessage(msg);
                }
            }
        });
        return true;
    }

    void send(BytesMessage message, Topic topic)
    {
        try
        {
            this.solaceTx.send(message, topic);
        }
        catch (ClosedFacilityException e)
        {
            Log.log(this, "Could not send ", ObjectUtils.safeToString(message), " to CLOSED ",
                ObjectUtils.safeToString(this));
        }
        catch (JCSMPException e)
        {
            Log.log(this,
                "Could not send " + ObjectUtils.safeToString(message) + " to " + ObjectUtils.safeToString(topic), e);
        }
    }

    synchronized void addSubscription(XMLMessageListener messageListener, Topic topic)
    {
        if (this.serviceTopicListeners.addSubscriberFor(topic.getName(), messageListener))
        {
            if (this.serviceTopicListeners.getSubscribersFor(topic.getName()).length == 1)
            {
                try
                {
                    Log.log(this, "Adding subscription for ", ObjectUtils.safeToString(topic));
                    this.session.addSubscription(topic);
                }
                catch (JCSMPException e)
                {
                    Log.log(this, "Could not subscribe for " + ObjectUtils.safeToString(topic), e);
                }
            }
        }
    }

    synchronized void removeSubscription(XMLMessageListener messageListener, Topic topic)
    {
        if (this.serviceTopicListeners.removeSubscriberFor(topic.getName(), messageListener))
        {
            if (this.serviceTopicListeners.getSubscribersFor(topic.getName()).length == 0)
            {
                try
                {
                    Log.log(this, "Removing subscription for ", ObjectUtils.safeToString(topic));
                    this.session.removeSubscription(topic);
                }
                catch (JCSMPException e)
                {
                    Log.log(this, "Could not unsubscribe for " + ObjectUtils.safeToString(topic), e);
                }
            }
        }
    }
}
