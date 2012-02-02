package com.netflix.jmeter.properties;

public class Properties
{
    public static final Properties instance = new Properties();
    public CassandraProperties cassandra;
    public SchemaProperties schema;
    public FatclientProperties fatclient;

    public boolean isCassandra()
    {
        return cassandra != null;
    }

    public boolean isSchema()
    {
        return schema != null;
    }

    public boolean isFatClient()
    {
        return fatclient != null;
    }
}
