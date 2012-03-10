package com.netflix.jmeter.connections.a6x;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
//import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.shallows.EmptyLatencyScoreStrategyImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;

public class AstyanaxConnection extends Connection
{
    private static final Logger logger = LoggerFactory.getLogger(AstyanaxConnection.class);
    public static final AstyanaxConnection instance = new AstyanaxConnection();
    public Properties config = new Properties();
    private Keyspace keyspace;
    //private ConnectionPoolMonitor connectionPoolMonitor = new Slf4jConnectionPoolMonitorImpl();;
    private ConnectionPoolMonitor connectionPoolMonitor = new CountingConnectionPoolMonitor();

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

                AstyanaxConfigurationImpl configuration = new AstyanaxConfigurationImpl();
                configuration.setDiscoveryType(NodeDiscoveryType.valueOf(config.getProperty("astyanax.connection.discovery", "NONE")));
                configuration.setConnectionPoolType(ConnectionPoolType.valueOf(config.getProperty("astyanax.connection.pool", "ROUND_ROBIN")));
                configuration.setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf(com.netflix.jmeter.properties.Properties.instance.cassandra.getReadConsistency()));
                configuration.setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf(com.netflix.jmeter.properties.Properties.instance.cassandra.getWriteConsistency()));

                logger.info("AstyanaxConfiguration: " + configuration.toString());

                String property = config.getProperty("astyanax.connection.latency.stategy", "EmptyLatencyScoreStrategyImpl");
                LatencyScoreStrategy latencyScoreStrategy = null;
                if (property.equalsIgnoreCase("SmaLatencyScoreStrategyImpl"))
                {
                    int updateInterval = Integer.parseInt(config.getProperty("astyanax.connection.latency.stategy.updateInterval", "2000"));
                    int resetInterval = Integer.parseInt(config.getProperty("astyanax.connection.latency.stategy.resetInterval", "10000"));
                    int windowSize = Integer.parseInt(config.getProperty("astyanax.connection.latency.stategy.windowSize", "100"));
                    double badnessThreshold = Double.parseDouble(config.getProperty("astyanax.connection.latency.stategy.badnessThreshold", "0.1"));
                    // latencyScoreStrategy = new SmaLatencyScoreStrategyImpl(updateInterval, resetInterval, windowSize, badnessThreshold);
                }
                else
                {
                    latencyScoreStrategy = new EmptyLatencyScoreStrategyImpl();
                }

                String maxConnection = com.netflix.jmeter.properties.Properties.instance.cassandra.getMaxConnsPerHost();
                ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl(getClusterName()).setPort(port);
                poolConfig.setMaxConnsPerHost(Integer.parseInt(maxConnection));
                poolConfig.setSeeds(StringUtils.join(endpoints, ":" + port + ","));
                poolConfig.setLatencyScoreStrategy(latencyScoreStrategy);
                
                logger.info("ConnectionPoolConfiguration: " + poolConfig.toString());
                
                // set this as field for logging purpose only.
                Builder builder = new AstyanaxContext.Builder();
                builder.forCluster(getClusterName());
                builder.forKeyspace(getKeyspaceName());
                builder.withAstyanaxConfiguration(configuration);
                builder.withConnectionPoolConfiguration(poolConfig);
                builder.withConnectionPoolMonitor(connectionPoolMonitor);
                builder.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

                AstyanaxContext<Keyspace> context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
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

    @Override
    public String logConnections()
    {
        return connectionPoolMonitor == null ? "" : connectionPoolMonitor.toString();
    }
}
