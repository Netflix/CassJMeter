package com.netflix.jmeter.connections.fatclient;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;

import com.google.common.collect.Lists;
import com.netflix.jmeter.connections.thrift.ThriftOperation;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.utils.SystemUtils;

public class FatClientOperation extends ThriftOperation
{
    private String cf;
    private String ks;

    public FatClientOperation(String writeConsistency, String readConsistency, String ks, String cf)
    {
        super(null, writeConsistency, readConsistency, cf);
        this.cf = cf;
        this.ks = ks;
    }

    @Override
    public ResponseData put(Object key, Object colName, Object value) throws OperationException
    {
        ByteBuffer rKey = kser.toByteBuffer(key);
        ByteBuffer name = colser.toByteBuffer(colName);
        ByteBuffer val = valser.toByteBuffer(value);
        RowMutation change = new RowMutation(ks, rKey);
        ColumnPath cp = new ColumnPath(cf).setColumn(name);
        change.add(new QueryPath(cp), val, System.currentTimeMillis());
        try
        {
            StorageProxy.mutate(Arrays.asList(change), wConsistecy);
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new ResponseData("", 0, "");
    }

    @Override
    public ResponseData batchMutate(Object key, Map<?, ?> nv) throws OperationException
    {
        ByteBuffer rKey = kser.toByteBuffer(key);
        RowMutation change = new RowMutation(ks, rKey);
        for (Map.Entry entry : nv.entrySet())
        {
            ByteBuffer name = colser.toByteBuffer(entry.getKey());
            ByteBuffer val = valser.toByteBuffer(entry.getValue());
            ColumnPath cp = new ColumnPath(cf).setColumn(name);
            change.add(new QueryPath(cp), val, System.currentTimeMillis());
        }
        try
        {
            StorageProxy.mutate(Arrays.asList(change), wConsistecy);
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new ResponseData("", 0, "");
    }

    @Override
    public ResponseData get(Object rkey, Object colName) throws OperationException
    {
        ByteBuffer rKey = kser.toByteBuffer(rkey);
        ByteBuffer name = colser.toByteBuffer(colName);

        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        Collection<ByteBuffer> cols = Lists.newArrayList(name);
        SliceByNamesReadCommand readCommand = new SliceByNamesReadCommand(ks, rKey, new QueryPath(cf, null, null), cols);
        readCommand.setDigestQuery(false);
        commands.add(readCommand);
        List<Row> rows;
        try
        {
            rows = StorageProxy.read(commands, rConsistecy);
            Row row = rows.get(0);
            ColumnFamily cf = row.cf;

            int bytes = 0;
            StringBuffer response = new StringBuffer();
            if (cf != null)
            {
                for (IColumn col : cf.getSortedColumns())
                {
                    String value = SystemUtils.convertToString(valser, col.value());
                    response.append(colser.fromByteBuffer(col.name())).append(":").append(value);
                    bytes += col.name().capacity();
                    bytes += col.value().capacity();
                }
            }
            return new ResponseData(response.toString(), bytes, "");
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
    }

    @Override
    public ResponseData rangeSlice(Object rkey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException
    {
        ByteBuffer rKey = kser.toByteBuffer(rkey);
        ByteBuffer sname = colser.toByteBuffer(startColumn);
        ByteBuffer ename = colser.toByteBuffer(endColumn);
        
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        ReadCommand readCommand = new SliceFromReadCommand(ks, rKey, new ColumnParent(cf), sname, ename, reversed, count);
        readCommand.setDigestQuery(false);
        commands.add(readCommand);
        List<Row> rows;
        try
        {
            rows = StorageProxy.read(commands, rConsistecy);
            Row row = rows.get(0);
            ColumnFamily cf = row.cf;

            int bytes = 0;
            StringBuffer response = new StringBuffer();
            if (cf != null)
            {
                for (IColumn col : cf.getSortedColumns())
                {
                    String value = SystemUtils.convertToString(valser, col.value());
                    response.append(colser.fromByteBuffer(col.name())).append(":").append(value);
                    bytes += col.name().capacity();
                    bytes += col.value().capacity();
                }
            }
            return new ResponseData(response.toString(), bytes, "");
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
    }
}
