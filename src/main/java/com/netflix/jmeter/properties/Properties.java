package com.netflix.jmeter.properties;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.jmeter.connections.a6x.AstyanaxConnection;

public class Properties
{
    private static final Logger logger = LoggerFactory.getLogger(AstyanaxConnection.class);
    public static final Properties instance = new Properties();
    public CassandraProperties cassandra;
    public FatclientProperties fatclient;
    private List<SchemaProperties> schemas = new ArrayList<SchemaProperties>();

    public void addSchema(SchemaProperties newProp)
    {
        schemas.add(newProp);
        logger.info("Queing schema change for the cf: {}", newProp);
    }
    
    public List<SchemaProperties> getSchemas()
    {
        return schemas;
    }
}
