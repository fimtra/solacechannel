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

import com.fimtra.channel.EndPointAddress;
import com.fimtra.channel.IEndPointService;
import com.fimtra.channel.IEndPointServiceBuilder;
import com.fimtra.channel.IReceiver;

/**
 * A builder for a {@link SolaceService} instance
 * 
 * @author Ramon Servadei
 */
public final class SolaceServiceBuilder implements IEndPointServiceBuilder
{
    static final SolaceMultiplexSession multiplexSession = new SolaceMultiplexSession();

    final EndPointAddress endPointAddress;

    public SolaceServiceBuilder(EndPointAddress endPointAddress)
    {
        this.endPointAddress = endPointAddress;
    }

    @Override
    public IEndPointService buildService(IReceiver receiver)
    {
        return new SolaceService(multiplexSession, this.endPointAddress, receiver);
    }

}
