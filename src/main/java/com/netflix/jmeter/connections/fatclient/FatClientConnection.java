package com.netflix.jmeter.connections.fatclient;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.jmeter.properties.Properties;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.utils.YamlUpdater;

public class FatClientConnection extends Connection
{
    private static final Logger logger = LoggerFactory.getLogger(FatClientConnection.class);

    public FatClientConnection()
    {
        super();
        try
        {
            // update yaml for the test case.
            updateYaml();
            // start the fat client.
            startClient();
        }
        catch (Exception ex)
        {
            logger.error("Couldnt Start the client because of:", ex);
            throw new RuntimeException(ex);
        }
    }

    private void updateYaml() throws IOException
    {
        YamlUpdater updater = new YamlUpdater("cassandra.yaml");
        updater.update("listen_address", null);
        updater.update("rpc_address", null);
        updater.update("storage_port", 7101);
        updater.update("rpc_port", port);
        updater.update("cluster_name", Properties.instance.cassandra.getClusterName());
        updater.update("endpoint_snitch", Properties.instance.fatclient.getEndpoint_Snitch());
        updater.setSeeds(endpoints);
        updater.update("dynamic_snitch", Properties.instance.fatclient.getDynamic_snitch());
        updater.update("rpc_timeout_in_ms", Properties.instance.fatclient.getRpc_timeout_in_ms());
        updater.update("dynamic_snitch", Properties.instance.fatclient.getDynamic_snitch());
        updater.encriptionOption("internode_encryption", Properties.instance.fatclient.getInternode_encryption());
        updater.dump();
    }

    private static void startClient() throws Exception
    {
        try
        {
            if (!System.getProperties().contains("cassandra.config"))
            {
                String url = "file:///" + new File("cassandra.yaml").getAbsolutePath();
                System.getProperties().setProperty("cassandra.config", url);
            }
            StorageService.instance.initClient();
            // sleep for a bit so that gossip can do its thing.
            Thread.sleep(10000L);
        }
        catch (Exception ex)
        {
            logger.error("Couldnt Start the client because of:", ex);
            throw new AssertionError(ex);
        }
        catch (Throwable ex)
        {
            logger.error("Couldnt Start the client because of:", ex);
            throw new AssertionError(ex);
        }
    }

    @Override
    public Operation newOperation(String columnName, boolean isCounter)
    {
        return new FatClientOperation(Properties.instance.cassandra.getWriteConsistency(), 
                Properties.instance.cassandra.getReadConsistency(), 
                Properties.instance.cassandra.getKeyspace(), 
                columnName);
    }
}
