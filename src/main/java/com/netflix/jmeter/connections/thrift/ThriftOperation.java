package com.netflix.jmeter.connections.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.utils.Pair;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.utils.CClient;
import com.netflix.jmeter.utils.SystemUtils;

public class ThriftOperation implements Operation
{
    private final String cfName;
    protected ConsistencyLevel wConsistecy;
    protected ConsistencyLevel rConsistecy;
    protected AbstractSerializer colser;
    protected AbstractSerializer valser;
    protected AbstractSerializer kser;
    private CClient client;

    public ThriftOperation(CClient client, String writeConsistency, String readConsistency, String cfName)
    {
        this.client = client;
        this.wConsistecy = ConsistencyLevel.valueOf(writeConsistency);
        this.rConsistecy = ConsistencyLevel.valueOf(readConsistency);
        this.cfName = cfName;
    }

    @Override
    public void serlizers(AbstractSerializer<?> kser, AbstractSerializer<?> colser, AbstractSerializer<?> valser)
    {
        this.kser = kser;
        this.colser = colser;
        this.valser = valser;
    }

    @Override
    public ResponseData put(Object key, Object colName, Object value) throws OperationException
    {
        ByteBuffer rKey = kser.toByteBuffer(key);
        ByteBuffer name = colser.toByteBuffer(colName);
        ByteBuffer val = valser.toByteBuffer(value);
        try
        {
            new Writer(client, wConsistecy, cfName).insert(rKey, name, val);
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new ResponseData("", 0, client.host, 0, key, colName, value);
    }

    @Override
    public ResponseData batchMutate(Object key, Map<?, ?> nv) throws OperationException
    {
        Writer writer = new Writer(client, wConsistecy, cfName);
        try
        {
            for (Map.Entry<?, ?> entity : nv.entrySet())
            {
                ByteBuffer name = colser.toByteBuffer(entity.getKey());
                ByteBuffer value = valser.toByteBuffer(entity.getValue());
                writer.prepareAdd(name, value);
            }
            writer.insert(kser.toByteBuffer(key));
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new ResponseData("", 0, client.host, 0, key, nv);
    }

    @Override
    public ResponseData get(Object rkey, Object colName) throws OperationException
    {
        ByteBuffer rKey = kser.toByteBuffer(rkey);
        ByteBuffer name = colser.toByteBuffer(colName);
        String response;
        int bytes = 0;
        try
        {
            ByteBuffer value = new Reader(client, rConsistecy, cfName).get(rKey, name).getColumn().value;
            response = SystemUtils.convertToString(valser, value);
            bytes = value.capacity();
        }
        catch (NotFoundException e)
        {
            response = ".... Not Found ...";
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new ResponseData(response, bytes, client.host, 0, rkey, colName, null);
    }

    @Override
    public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException
    {
        ByteBuffer key = kser.toByteBuffer(rKey);
        ByteBuffer start = colser.toByteBuffer(startColumn);
        ByteBuffer end = colser.toByteBuffer(endColumn);
        StringBuffer response = new StringBuffer();
        int bytes = 0;
        try
        {
            long s = System.currentTimeMillis();
            List<ColumnOrSuperColumn> reader = new Reader(client, rConsistecy, cfName).getSlice(key, start, end, count, reversed);
            for (ColumnOrSuperColumn col : reader)
            {
                byte[] name = col.getColumn().getName();
                bytes += name.length;
                ByteBuffer value = col.getColumn().value;
                bytes += value.capacity();
                String valueString = SystemUtils.convertToString(valser, value);
                response.append(colser.fromBytes(name).toString()).append(":").append(valueString).append("\n");
            }
        }
        catch (NotFoundException e)
        {
            response.append(".... Not Found ...");
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new ResponseData(response.toString(), bytes, client.host, 0, rKey, Pair.create(startColumn, endColumn), null);
    }

    @Override
    public ResponseData putComposite(String key, String colName, ByteBuffer vbb) throws OperationException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResponseData batchCompositeMutate(String key, Map<String, ByteBuffer> nv) throws OperationException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResponseData getComposite(String stringValue, String stringValue2) throws OperationException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ResponseData delete(Object rkey, Object colName) throws OperationException
    {
        // TODO Auto-generated method stub
        return null;
    }
}
