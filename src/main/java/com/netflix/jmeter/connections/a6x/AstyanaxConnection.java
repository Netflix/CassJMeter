package com.netflix.jmeter.connections.a6x;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;

public class AstyanaxConnection extends Connection
{
    public static AstyanaxConnection instance = new AstyanaxConnection();
    public Properties config = new Properties();
    private Keyspace keyspace;

    public Keyspace keyspace()
    {
        if (keyspace != null)
            return keyspace;

        synchronized (AstyanaxConnection.class)
        {
            // double check...
            if (keyspace != null)
                return keyspace;
            try
            {
                AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                        .forCluster(getClusterName())
                        .forKeyspace(getKeyspaceName())
                        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                        .withConnectionPoolConfiguration(
                                new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(port).setMaxConnsPerHost(1).setSeeds(StringUtils.join(endpoints, ":" + port + ",")))
                        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).buildKeyspace(ThriftFamilyFactory.getInstance());
                context.start();
                keyspace = context.getEntity();
                return keyspace;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public Operation newOperation(String columnName)
    {
        return new AstyanaxOperation(columnName);
    }
}
