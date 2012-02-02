package com.netflix.jmeter.connections.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;

import com.google.common.collect.Lists;
import com.netflix.jmeter.utils.CClient;

public class Writer
{
    private CClient client;
    private ConsistencyLevel cl;
    private final String cfName;
    List<Column> columns = Lists.newArrayList();

    public Writer(CClient client, ConsistencyLevel cl, String cfName)
    {
        this.client = client;
        this.cl = cl;
        this.cfName = cfName;
    }

    public void insert(ByteBuffer key, ByteBuffer name, ByteBuffer value) throws Exception
    {
        Column col = new Column(name).setValue(value).setTimestamp(System.nanoTime());
        ColumnParent cp = new ColumnParent(cfName);
        client.insert(key, cp, col, cl);
    }

    public Writer prepareAdd(ByteBuffer name, ByteBuffer value) throws Exception
    {
        Column col = new Column(name).setValue(value).setTimestamp(System.nanoTime());
        columns.add(col);
        return this;
    }

    public void insert(ByteBuffer key) throws Exception
    {
        assert columns.size() != 0;
        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        record.put(key, getColumnsMutationMap(columns));
        client.batch_mutate(record, cl);
    }

    private Map<String, List<Mutation>> getColumnsMutationMap(List<Column> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }
        mutationMap.put(cfName, mutations);
        return mutationMap;
    }
}
