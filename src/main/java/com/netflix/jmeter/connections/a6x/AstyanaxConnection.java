package com.netflix.jmeter.connections.a6x;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftClusterImpl;
import com.netflix.cassandra.NetflixConnectionPoolMonitor;
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
                com.netflix.jmeter.properties.Properties.instance.cassandra.addProperties(config);
                File propFile = new File("cassandra.properties");
                if (propFile.exists())
                    config.load(new FileReader(propFile));
                ThriftClusterImpl cluster = loadProps();
                cluster.start();
                keyspace = cluster.getKeyspace(getKeyspaceName());
                return keyspace;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public ThriftClusterImpl loadProps()
    {
        ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl(getClusterName(), null);
        config.setPort(port);
        config.setSocketTimeout(30000);
        config.setMaxTimeoutWhenExhausted(200);
        config.setIsDebugEnabled(true);
        String stringPort = com.netflix.jmeter.properties.Properties.instance.cassandra.getMaxConnsPerHost();
        config.setMaxConnsPerHost(Integer.parseInt(stringPort));
        config.setConnectionPoolMonitor(new NetflixConnectionPoolMonitor(getClusterName() + ":" + getKeyspaceName(), config));
        config.setSeeds(StringUtils.join(endpoints, ","));
        return new ThriftClusterImpl(config);
    }

    public Operation newOperation()
    {
        return new AstyanaxOperation();
    }
}
