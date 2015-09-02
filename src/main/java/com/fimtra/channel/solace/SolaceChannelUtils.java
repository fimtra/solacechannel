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

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPFactory;

/**
 * Utilities for working with {@link SolaceChannel} instances
 * 
 * @author Ramon Servadei
 */
public abstract class SolaceChannelUtils
{
    static final JCSMPFactory JCSMP_FACTORY = JCSMPFactory.onlyInstance();

    static final Deque<BytesMessage> pool = new LinkedList<BytesMessage>();
    static final Lock lock = new ReentrantLock();

    static BytesMessage getPooledBytesMessage()
    {
        lock.lock();
        try
        {
            if (pool.size() == 0)
            {
                return SolaceChannelUtils.JCSMP_FACTORY.createMessage(BytesMessage.class);
            }
            return pool.removeFirst();
        }
        finally
        {
            lock.unlock();
        }
    }

    static void returnPooledBytesMessage(BytesMessage msg)
    {
        lock.lock();
        try
        {
            msg.reset();
            pool.add(msg);
        }
        finally
        {
            lock.unlock();
        }
    }
}
