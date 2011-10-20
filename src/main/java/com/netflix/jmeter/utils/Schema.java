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
    private String cfName;

    public Schema(CClient client)
    {
        this.client = client;
        this.ksName = Connection.getKeyspaceName();
        this.cfName = Connection.getColumnFamilyName();
    }

    public void createKeyspace() throws Exception
    {
        // create Keyspace if it doesnt exist.
        KsDef ksd;
        try
        {
            ksd = client.describe_keyspace(ksName);
        }
        catch (NotFoundException ex)
        {
            ksd = new KsDef(ksName, STATEGY_CLASS, new ArrayList<CfDef>());
            Map<String, String> strategy_options = Maps.newHashMap();
            String[] splits = Properties.instance.schema.getStrategy_options().split(",");
            for (String split : splits)
            {
                String[] replication = split.split(":");
                assert replication.length == 2;
                strategy_options.put(replication[0], replication[1]);
            }
            ksd.setStrategy_options(strategy_options);
            client.send_system_add_keyspace(ksd);
        }
        createColumnFamily(ksd);
    }

    public void createColumnFamily(KsDef ksd) throws Exception
    {
        // create column family
        List<CfDef> list = ksd.getCf_defs() == null ? new ArrayList<CfDef>() : ksd.getCf_defs();
        for (CfDef cfd : list)
        {
            if (cfd.getName().equals(cfName))
                return;
        }
        client.set_keyspace(ksName);
        client.send_system_add_column_family(columnFamilyDef());
    }

    private CfDef columnFamilyDef()
    {
        SchemaProperties prop = Properties.instance.schema;
        CfDef cfd = new CfDef(ksName, cfName);
        cfd.setMemtable_throughput_in_mb(128);
        cfd.setKey_cache_size(1.0);
        cfd.setComparator_type(prop.getComparator_type());
        cfd.setKey_validation_class(prop.getKey_validation_class());
        cfd.setDefault_validation_class(prop.getDefault_validation_class());
        cfd.setKey_cache_size(1D);
        cfd.setRow_cache_provider(prop.getRow_cache_provider());
        cfd.setRow_cache_size(Double.parseDouble(prop.getRows_cached()));
        cfd.setMemtable_flush_after_mins(Integer.parseInt(prop.getMemtable_flush_after()));
        cfd.setMemtable_throughput_in_mb(Integer.parseInt(prop.getMemtable_throughput()));
        return cfd;
    }
}
