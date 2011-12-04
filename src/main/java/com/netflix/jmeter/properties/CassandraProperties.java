package com.netflix.jmeter.properties;

import java.util.Properties;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.testbeans.TestBean;

public class CassandraProperties extends ConfigTestElement implements TestBean
{
    private static final long serialVersionUID = 468255622613306730L;
    private static final String Keyspace = "keyspace";
    private static final String ClusterName = "clusterName";
    private static final String ReadConsistency = "readConsistency";
    private static final String WriteConsistency = "writeConsistency";
    private static final String CassandraServers = "cassandraServers";
    private static final String ClientType = "clientType";
    public static final String MaxConnectionsPerHost = "maxConnsPerHost";

    public CassandraProperties()
    {
        com.netflix.jmeter.properties.Properties.instance.cassandra = this;
    }

    public String prefixPropertyName(String name)
    {
        return getPropertyAsString(CassandraProperties.ClusterName) + "." + getPropertyAsString(CassandraProperties.Keyspace) + name;
    }

    public String getClientType()
    {
        return getPropertyAsString(ClientType);
    }

    public void setClientType(String clientType)
    {
        setProperty(ClientType, clientType);
    }

    public String getKeyspace()
    {
        return getPropertyAsString(Keyspace);
    }

    public void setKeyspace(String keyspace)
    {
        setProperty(Keyspace, keyspace);
    }

    public String getClusterName()
    {
        return getPropertyAsString(ClusterName);
    }

    public void setClusterName(String clusterName)
    {
        setProperty(ClusterName, clusterName);
    }

    public String getReadConsistency()
    {
        return getPropertyAsString(ReadConsistency);
    }

    public void setReadConsistency(String readConsistency)
    {
        setProperty(ReadConsistency, readConsistency);
    }

    public String getWriteConsistency()
    {
        return getPropertyAsString(WriteConsistency);
    }

    public void setWriteConsistency(String writeConsistency)
    {
        setProperty(WriteConsistency, writeConsistency);
    }

    public String getCassandraServers()
    {
        return getPropertyAsString(CassandraServers);
    }

    public void setCassandraServers(String cassandraServers)
    {
        setProperty(CassandraServers, cassandraServers);
    }

    public String getMaxConnsPerHost()
    {
        return getPropertyAsString(MaxConnectionsPerHost);
    }

    public void setMaxConnsPerHost(String connections)
    {
        setProperty(MaxConnectionsPerHost, connections);
    }

    public void addProperties(Properties prop)
    {
        prop.put("jmeter.cluster", getClusterName());
        prop.put("jmeter.keyspace", getKeyspace());
        prop.put(prefixPropertyName(".astyanax.writeConsistency"), getWriteConsistency());
        prop.put(prefixPropertyName(".astyanax.servers"), getCassandraServers());
        prop.put(prefixPropertyName(".astyanax.readConsistency"), getReadConsistency());
        prop.put(prefixPropertyName(".astyanax.maxConnsPerHost"), getMaxConnsPerHost());
    }
}