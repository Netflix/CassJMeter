package com.netflix.jmeter.sampler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.netflix.jmeter.properties.Properties;
import com.netflix.jmeter.utils.CClient;
import com.netflix.jmeter.utils.Schema;

public abstract class Connection
{
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    public static volatile boolean intialized = false;
    public static Connection connection;

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    public volatile Set<String> endpoints;
    public int port = 0;

    public Connection()
    {
        // parse the seeds from the property.
        parseSeeds();
        // schedule the describe ring.
        scheduleDescribeRing();
        // setup for your use first.
        setupKeyspace();
    }

    /**
     * This method will parse the seed property from the test case.
     */
    void parseSeeds()
    {
        Set<String> temp = Sets.newHashSet();
        for (String host : Properties.instance.cassandra.getCassandraServers().split(","))
        {
            String[] hp = host.split(":");
            temp.add(hp[0]);
            port = Integer.parseInt(hp[1]);
        }
        assert temp.size() == 0;
        endpoints = temp;
    }

    void setupKeyspace()
    {
        if (Properties.instance.getSchemas().size() == 0)
            return;
        for (String host : endpoints)
        {
            try
            {
                CClient c = CClient.getClient(host, port);
                new Schema(c).createKeyspace();
                c.socket.close();
                break;
            }
            catch (Exception unlucky)
            {
                logger.error("Error talking to the client: ", unlucky);
            }
        }
    }

    void scheduleDescribeRing()
    {
        if (endpoints.size() > 1)
            return;
        // sleep for 2 Min
        executor.schedule(new Runnable()
        {
            public void run()
            {
                describeRing();
            }
        }, 2, TimeUnit.MINUTES);
    }

    void describeRing()
    {
        try
        {
            CClient client = CClient.getClient(endpoints.iterator().next(), port);
            client.set_keyspace(Properties.instance.cassandra.getKeyspace());
            // get the nodes in the ring.
            List<TokenRange> lt = client.describe_ring(getKeyspaceName());
            Set<String> temp = Sets.newHashSet();
            for (TokenRange range : lt)
                temp.addAll(range.endpoints);
            endpoints = temp;
            // TODO: filter out the nodes in the other region.
            client.socket.close();
        }
        catch (Exception wtf)
        {
            throw new RuntimeException(wtf);
        }
    }

    public static Connection getInstance()
    {
        if (connection != null)
            return connection;
        synchronized (Connection.class)
        {
            if (connection != null)
                return connection;
            try
            {
                connection = FBUtilities.construct(Properties.instance.cassandra.getClientType(), "Creating Connection");
                // Log the metrics for troubleshooting. 
                Thread t = new Thread()
                {
                    @Override
                    public void run()
                    {
                        while (true)
                        {
                            try
                            {
                                logger.info("ConnectionPoolMonitor: " + connection.logConnections());
                                Thread.sleep(60 * 1000);
                            }
                            catch (InterruptedException wtf)
                            {
                                // Ignored.
                            }
                        }
                    }
                };
                t.setDaemon(true);
                t.start();
                return connection;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static String getKeyspaceName()
    {
        return Properties.instance.cassandra.getKeyspace();
    }

    public static String getClusterName()
    {
        return Properties.instance.cassandra.getClusterName();
    }

    public static ColumnFamilyType getColumnFamilyType()
    {
        return ColumnFamilyType.Standard;
    }

    public abstract Operation newOperation(String columnName, boolean isCounter);
    
    public abstract String logConnections();

    public abstract void shutdown();
}
