package com.netflix.jmeter.connections.thrift;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

public class Counter
{
    private Client client;
    private ConsistencyLevel cl;
    private final String cfName;

    public Counter(Cassandra.Client client, ConsistencyLevel cl, String cfName)
    {
        this.client = client;
        this.cl = cl;
        this.cfName = cfName;
    }

    public void add(ByteBuffer rawKey, List<CounterColumn> columns) throws Exception
    {
        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        record.put(rawKey, getColumnsMutationMap(columns));
        client.batch_mutate(record, cl);
    }

    public List<ColumnOrSuperColumn> get(ByteBuffer keyBuffer, ByteBuffer start, ByteBuffer finish, int order) throws Exception
    {
        SliceRange sliceRange = new SliceRange();
        // start/finish
        sliceRange.setStart(start).setFinish(finish);
        // reversed/count
        sliceRange.setReversed(false).setCount(order);
        // initialize SlicePredicate with existing SliceRange
        SlicePredicate predicate = new SlicePredicate().setSlice_range(sliceRange);
        ColumnParent parent = new ColumnParent(cfName);
        return client.get_slice(keyBuffer, parent, predicate, cl);
    }

    private Map<String, List<Mutation>> getColumnsMutationMap(List<CounterColumn> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
        for (CounterColumn c : columns)
        {
            ColumnOrSuperColumn cosc = new ColumnOrSuperColumn().setCounter_column(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(cosc));
        }
        mutationMap.put(cfName, mutations);
        return mutationMap;
    }
}
