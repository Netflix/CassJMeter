package com.netflix.jmeter.connections.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.netflix.jmeter.utils.CClient;

public class Reader
{
    private CClient client;
    private ConsistencyLevel cl;
    private final String cfName;

    public Reader(CClient client, ConsistencyLevel cl, String cfName)
    {
        this.client = client;
        this.cl = cl;
        this.cfName = cfName;
    }

    public List<KeySlice> indexGet(ByteBuffer columnName, ByteBuffer value, ByteBuffer startOffset, int limit, boolean isReverse) throws Exception
    {
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, isReverse, limit));
        ColumnParent parent = new ColumnParent(cfName);
        IndexExpression expression = new IndexExpression(columnName, IndexOperator.EQ, value);
        IndexClause clause = new IndexClause(Arrays.asList(expression), startOffset, limit);
        return client.get_indexed_slices(parent, clause, predicate, cl);
    }

    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiGet(List<ByteBuffer> keys, int limit) throws Exception
    {
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, limit));
        ColumnParent parent = new ColumnParent(cfName);
        return client.multiget_slice(keys, parent, predicate, cl);
    }

    public List<KeySlice> getRangeSlice(ByteBuffer start, ByteBuffer end, int limit) throws Exception
    {
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange(ByteBufferUtil.EMPTY_BYTE_BUFFER, ByteBufferUtil.EMPTY_BYTE_BUFFER, false, limit));
        ColumnParent parent = new ColumnParent(cfName);
        KeyRange range = new KeyRange(limit).setStart_key(start).setEnd_key(end);
        return client.get_range_slices(parent, predicate, range, cl);
    }

    public List<ColumnOrSuperColumn> getSlice(ByteBuffer key, ByteBuffer start, ByteBuffer end, int limit, boolean isReverse) throws Exception
    {
        SliceRange sliceRange = new SliceRange().setStart(start).setFinish(end).setReversed(isReverse).setCount(limit);
        // initialize SlicePredicate with existing SliceRange
        SlicePredicate predicate = new SlicePredicate().setSlice_range(sliceRange);
        ColumnParent parent = new ColumnParent(cfName);
        return client.get_slice(key, parent, predicate, cl);
    }

    public ColumnOrSuperColumn get(ByteBuffer key, ByteBuffer column) throws Exception
    {
        ColumnPath cp = new ColumnPath().setColumn_family(cfName).setColumn(column);
        return client.get(key, cp, cl);
    }

}
