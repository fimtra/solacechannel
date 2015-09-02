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
import com.fimtra.channel.IReceiver;
import com.fimtra.channel.ITransportChannel;
import com.fimtra.channel.ITransportChannelBuilder;

/**
 * A builder for a {@link SolaceChannel}
 * 
 * @author Ramon Servadei
 */
public final class SolaceChannelBuilder implements ITransportChannelBuilder
{
    static final SolaceMultiplexSession multiplexSession = new SolaceMultiplexSession();

    final EndPointAddress endPointAddress;

    public SolaceChannelBuilder(EndPointAddress endPoint)
    {
        super();
        this.endPointAddress = endPoint;
    }

    @Override
    public ITransportChannel buildChannel(IReceiver receiver) throws IOException
    {
        return new SolaceChannel(multiplexSession, SolaceService.createEndPointServiceTopic(
            this.endPointAddress.getNode(), this.endPointAddress.getPort()), receiver);
    }

    @Override
    public EndPointAddress getEndPointAddress()
    {
        return this.endPointAddress;
    }

    @Override
    public String toString()
    {
        return "SolaceChannelBuilder [" + this.endPointAddress + "]";
    }
}
