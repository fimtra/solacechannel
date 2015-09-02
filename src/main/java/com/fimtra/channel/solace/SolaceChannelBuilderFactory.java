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

import com.fimtra.channel.IEndPointAddressFactory;
import com.fimtra.channel.ITransportChannelBuilder;
import com.fimtra.channel.ITransportChannelBuilderFactory;
import com.solacesystems.jcsmp.JCSMPProperties;

/**
 * A factory that creates builders for {@link SolaceChannel} instances
 * 
 * @author Ramon Servadei
 */
public final class SolaceChannelBuilderFactory implements ITransportChannelBuilderFactory
{
    final JCSMPProperties properties;
    final IEndPointAddressFactory endPoints;

    public SolaceChannelBuilderFactory(IEndPointAddressFactory endPoints)
    {
        super();
        this.endPoints = endPoints;
        this.properties = new JCSMPProperties();
        try
        {
            Properties p = new Properties();
            p.load(this.getClass().getResourceAsStream("/solace-channel.properties"));

            Map.Entry<Object, Object> entry = null;
            Object key = null;
            Object value = null;
            for (Iterator<Map.Entry<Object, Object>> it = p.entrySet().iterator(); it.hasNext();)
            {
                entry = it.next();
                key = entry.getKey();
                value = entry.getValue();
                this.properties.setProperty(key.toString(), value);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not load solace settings from solace-channel.properties file", e);
        }

    }

    @Override
    public ITransportChannelBuilder nextBuilder()
    {
        return new SolaceChannelBuilder(this.endPoints.next());
    }

}
