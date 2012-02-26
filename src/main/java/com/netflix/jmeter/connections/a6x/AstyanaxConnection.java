package com.netflix.jmeter.connections.a6x;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.shallows.EmptyLatencyScoreStrategyImpl;
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
                File propFile = new File("cassandra.properties");
                if (propFile.exists())
                    config.load(new FileReader(propFile));
                AstyanaxConfigurationImpl configuration = new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.valueOf(config.getProperty("astyanax.connection.discovery", "NONE")))
                        .setConnectionPoolType(ConnectionPoolType.valueOf(config.getProperty("astyanax.connection.pool", "ROUND_ROBIN")))
                        .setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf(com.netflix.jmeter.properties.Properties.instance.cassandra.getReadConsistency()))
                        .setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf(com.netflix.jmeter.properties.Properties.instance.cassandra.getWriteConsistency()));
                
                String property = config.getProperty("astyanax.connection.latency.stategy", "SmaLatencyScoreStrategyImpl");
                String maxConnection = com.netflix.jmeter.properties.Properties.instance.cassandra.getMaxConnsPerHost();
                ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl(getClusterName())
                                                .setPort(port)
                                                .setMaxConnsPerHost(Integer.parseInt(maxConnection))
                                                .setSeeds(StringUtils.join(endpoints, ":" + port + ","));
                if (property.equalsIgnoreCase("SmaLatencyScoreStrategyImpl"))
                    poolConfig.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl(poolConfig));
                else
                    poolConfig.setLatencyScoreStrategy(new EmptyLatencyScoreStrategyImpl());
                
                AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                        .forCluster(getClusterName())
                        .forKeyspace(getKeyspaceName())
                        .withAstyanaxConfiguration(configuration)
                        .withConnectionPoolConfiguration(poolConfig)
                        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                        .buildKeyspace(ThriftFamilyFactory.getInstance());
                
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

    public Operation newOperation(String columnName, boolean isCounter)
    {
        return new AstyanaxOperation(columnName, isCounter);
    }
}
