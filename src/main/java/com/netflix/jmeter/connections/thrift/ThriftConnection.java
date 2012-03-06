package com.netflix.jmeter.connections.thrift;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import com.netflix.jmeter.properties.Properties;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.utils.CClient;

public class ThriftConnection extends Connection
{
    public static final ThriftConnection instance = new ThriftConnection();
    public final ThreadLocal<CClient> clients = new ThreadLocal<CClient>()
    {
        @Override
        public CClient initialValue()
        {
            List<String> t = Lists.newArrayList(endpoints);
            Collections.shuffle(t);
            CClient client = CClient.getClient(t.get(0), port);
            try
            {
                client.set_keyspace(getKeyspaceName());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return client;
        }
    };

    @Override
    public Operation newOperation(String cfName, boolean iscounter)
    {
        return new ThriftOperation(clients.get(), 
                Properties.instance.cassandra.getWriteConsistency(), 
                Properties.instance.cassandra.getReadConsistency(),
                cfName);
    }

    @Override
    public String logConnections()
    {
        return "Nodes in the list: " + StringUtils.join(endpoints, ",");
    }
}
