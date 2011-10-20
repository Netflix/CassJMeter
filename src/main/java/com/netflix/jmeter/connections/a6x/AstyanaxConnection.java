package com.netflix.jmeter.connections.a6x;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import com.netflix.astyanax.Keyspace;
import com.netflix.cassandra.KeyspaceFactory;
import com.netflix.cassandra.NFAstyanaxManager;
import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.library.NFLibraryManager;

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
                NFLibraryManager.initLibrary(NFAstyanaxManager.class, config, true, false);
                keyspace = KeyspaceFactory.openKeyspace(getClusterName(), getKeyspaceName());
                return keyspace;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public Operation newOperation()
    {
        return new AstyanaxOperation();
    }
}
