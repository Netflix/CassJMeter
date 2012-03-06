package com.netflix.jmeter.connections.a6x;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Pair;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.ColumnMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.SerializerPackage;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.ByteBufferSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.utils.SystemUtils;

public class AstyanaxOperation implements Operation
{
    private AbstractSerializer valueSerializer;
    private ColumnFamily<Object, Object> cfs;
    private AbstractSerializer columnSerializer;
    private final String cfName;
    private final boolean isCounter;

    public class AstyanaxResponseData extends ResponseData
    {
        public AstyanaxResponseData(String response, int size, OperationResult<?> result)
        {
            super(response, size, EXECUTED_ON + result != null ? result.getHost().getHostName() : "", result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0);
        }

        public AstyanaxResponseData(String response, int size, OperationResult<?> result, Object key, Object cn, Object value)
        {
            super(response, size, EXECUTED_ON + result != null ? result.getHost().getHostName() : "", result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0, key, cn, value);
        }
        
        public AstyanaxResponseData(String response, int size, OperationResult<?> result, Object key, Map<?, ?> kv)
        {
            super(response, size, (result == null) ? "" : result.getHost().getHostName(), result != null ? result.getLatency(TimeUnit.MILLISECONDS) : 0, key, kv);
        }
    }
    
    AstyanaxOperation(String columnName, boolean isCounter)
    {
        this.cfName = columnName;
        this.isCounter = isCounter;
    }

    @Override
    public void serlizers(AbstractSerializer<?> keySerializer, AbstractSerializer<?> columnSerializer, AbstractSerializer<?> valueSerializer)
    {
        this.cfs = new ColumnFamily(cfName, keySerializer, columnSerializer);
        this.columnSerializer = columnSerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public ResponseData put(Object key, Object colName, Object value) throws OperationException
    {
        MutationBatch m = AstyanaxConnection.instance.keyspace().prepareMutationBatch();
        if (isCounter)
            m.withRow(cfs, key).incrementCounterColumn(colName, (Long) value);
        else
            m.withRow(cfs, key).putColumn(colName, value, valueSerializer, null);
        try
        {
            OperationResult<Void> result = m.execute();
            return new AstyanaxResponseData("", 0, result, key, colName, value);
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
    }

    @Override
    public ResponseData putComposite(String key, String colName, ByteBuffer value) throws OperationException
    {
        try
        {
            SerializerPackage sp = AstyanaxConnection.instance.keyspace().getSerializerPackage(cfName, false);
            // work around
            ByteBuffer rowKey = sp.keyAsByteBuffer(key);
            ByteBuffer column = sp.columnAsByteBuffer(colName);
            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = new ColumnFamily(cfName, ByteBufferSerializer.get(), ByteBufferSerializer.get());
            ColumnMutation mutation = AstyanaxConnection.instance.keyspace().prepareColumnMutation(columnFamily, rowKey, column);
            OperationResult<Void> result;
            if (isCounter)
                result = mutation.incrementCounterColumn(LongSerializer.get().fromByteBuffer(value)).execute();
            else
                result = mutation.putValue(value, null).execute();
            return new AstyanaxResponseData("", 0, result, key, colName, value);
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
    }

    @Override
    public ResponseData batchCompositeMutate(String key, Map<String, ByteBuffer> nv) throws OperationException
    {
        // TODO implement
        return null;
    }

    @Override
    public ResponseData batchMutate(Object key, Map<?, ?> nv) throws OperationException
    {
        MutationBatch m = AstyanaxConnection.instance.keyspace().prepareMutationBatch();
        ColumnListMutation<Object> cf = m.withRow(cfs, key);
        for (Map.Entry<?, ?> entry : nv.entrySet())
        {
            if (isCounter)
                cf.incrementCounterColumn(entry.getKey(), (Long) entry.getValue());
            else
                cf.putColumn(entry.getKey(), entry.getValue(), valueSerializer, null);
        }
        try
        {
            OperationResult<Void> result = m.execute();
            return new AstyanaxResponseData("", 0, result, key, nv);
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
    }

    @Override
    public ResponseData get(Object rkey, Object colName) throws OperationException
    {
        StringBuffer response = new StringBuffer();
        int bytes = 0;
        OperationResult<Column<Object>> opResult = null;
        try
        {
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(cfs).getKey(rkey).getColumn(colName).execute();
            bytes = opResult.getResult().getRawName().capacity();
            bytes += opResult.getResult().getByteBufferValue().capacity();
            String value = SystemUtils.convertToString(valueSerializer, opResult.getResult().getByteBufferValue());
            response.append(value);
        }
        catch (NotFoundException ex)
        {
            // ignore this because nothing is available to show
            response.append("...Not found...");
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }

        return new AstyanaxResponseData(response.toString(), bytes, opResult, rkey, colName, null);
    }

    @Override
    public ResponseData getComposite(String key, String colName) throws OperationException
    {
        StringBuffer response = new StringBuffer();
        int bytes = 0;
        OperationResult<Column<ByteBuffer>> opResult = null;
        try
        {
            SerializerPackage sp = AstyanaxConnection.instance.keyspace().getSerializerPackage(cfName, false);
            ByteBuffer bbName = sp.columnAsByteBuffer(colName);
            ByteBuffer bbKey = sp.keyAsByteBuffer(key);
            ColumnFamily<ByteBuffer, ByteBuffer> columnFamily = new ColumnFamily(cfName, ByteBufferSerializer.get(), ByteBufferSerializer.get());
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(columnFamily).getKey(bbKey).getColumn(bbName).execute();
            bytes = opResult.getResult().getByteBufferValue().capacity();
            bytes += opResult.getResult().getRawName().capacity();
            String value = SystemUtils.convertToString(valueSerializer, opResult.getResult().getByteBufferValue());
            response.append(value);
        }
        catch (NotFoundException ex)
        {
            // ignore this because nothing is available to show
            response.append("...Not found...");
        }
        catch (Exception e)
        {
            throw new OperationException(e);
        }
        return new AstyanaxResponseData(response.toString(), bytes, opResult, key, colName, null);
    }

    @Override
    public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException
    {
        int bytes = 0;
        OperationResult<ColumnList<Object>> opResult = null;
        StringBuffer response = new StringBuffer().append("\n");
        try
        {
            RangeBuilder rb = new RangeBuilder().setStart(startColumn, columnSerializer).setEnd(endColumn, columnSerializer).setLimit(count).setReversed(reversed);
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(cfs).getKey(rKey).withColumnRange(rb.build()).execute();
            Iterator<?> it = opResult.getResult().iterator();
            while (it.hasNext())
            {
                Column<?> col = (Column<?>) it.next();
                String key = SystemUtils.convertToString(columnSerializer, col.getRawName());
                bytes += col.getRawName().capacity();
                String value = SystemUtils.convertToString(valueSerializer, col.getByteBufferValue());
                bytes += col.getByteBufferValue().capacity();
                response.append(key).append(":").append(value).append(SystemUtils.NEW_LINE);
            }
        }
        catch (NotFoundException ex)
        {
            // ignore this because nothing is available to show
            response.append("...Not found...");
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
        return new AstyanaxResponseData(response.toString(), bytes, opResult, rKey, Pair.create(startColumn, endColumn), null);
    }

    @Override
    public ResponseData delete(Object rkey, Object colName) throws OperationException
    {
        try
        {
            OperationResult<Void> opResult = AstyanaxConnection.instance.keyspace().prepareColumnMutation(cfs, rkey, colName).deleteColumn().execute();
            return new AstyanaxResponseData("", 0, opResult, rkey, colName, null);
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }
    }
}
