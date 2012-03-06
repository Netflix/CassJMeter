package com.netflix.jmeter.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;

import com.google.common.collect.Maps;
import com.netflix.jmeter.properties.Properties;
import com.netflix.jmeter.properties.SchemaProperties;
import com.netflix.jmeter.sampler.Connection;

public class Schema
{
    private static String STATEGY_CLASS = "org.apache.cassandra.locator.NetworkTopologyStrategy";
    private CClient client;
    private String ksName;

    public Schema(CClient client)
    {
        this.client = client;
        this.ksName = Connection.getKeyspaceName();
    }

    public synchronized void createKeyspace() throws Exception
    {
        // create Keyspace if it doesnt exist.
        KsDef ksd;
        try
        {
            ksd = client.describe_keyspace(ksName);
            client.set_keyspace(ksName);
            createColumnFamily(ksd, false);
        }
        catch (NotFoundException ex)
        {
            ksd = new KsDef(ksName, STATEGY_CLASS, new ArrayList<CfDef>());
            Map<String, String> strategy_options = Maps.newHashMap();
            String[] splits = Properties.instance.getSchemas().get(0).getStrategy_options().split(",");
            for (String split : splits)
            {
                String[] replication = split.split(":");
                assert replication.length == 2;
                strategy_options.put(replication[0], replication[1]);
            }
            ksd.setStrategy_options(strategy_options);
            createColumnFamily(ksd, true);
            client.send_system_add_keyspace(ksd);
        }
    }

    public void createColumnFamily(KsDef ksd, boolean addToKS) throws Exception
    {
        Map<String, SchemaProperties> removedDuplicates = Maps.newConcurrentMap();
        for (SchemaProperties props :  Properties.instance.getSchemas())
            removedDuplicates.put(props.getColumn_family(), props);
        
        OUTER: for (SchemaProperties props : removedDuplicates.values())
        {
            List<CfDef> list = ksd.getCf_defs() == null ? new ArrayList<CfDef>() : ksd.getCf_defs();
            for (CfDef cfd : list)
            {
                if (cfd.getName().equals(props.getColumn_family()))
                    continue OUTER;
            }
            
            if (addToKS)
            {
                ksd.addToCf_defs(columnFamilyDef(props));
            }
            else
            {
                client.send_system_add_column_family(columnFamilyDef(props));
            }
        }
    }

    // create column family
    private CfDef columnFamilyDef(SchemaProperties prop)
    {
        CfDef cfd = new CfDef(ksName, prop.getColumn_family());
        cfd.setKey_cache_size(Double.parseDouble(prop.getKeys_cached()));
        cfd.setComparator_type(prop.getComparator_type());
        cfd.setKey_validation_class(prop.getKey_validation_class());
        cfd.setDefault_validation_class(prop.getDefault_validation_class());
        cfd.setRow_cache_provider(prop.getRow_cache_provider());
        cfd.setRow_cache_size(Double.parseDouble(prop.getRows_cached()));
        return cfd;
    }
}
