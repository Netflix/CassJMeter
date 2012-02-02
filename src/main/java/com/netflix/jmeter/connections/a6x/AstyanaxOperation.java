package com.netflix.jmeter.connections.a6x;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.utils.Hex;

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
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.utils.SystemUtils;

public class AstyanaxOperation implements Operation
{
    private AbstractSerializer valser;
    private ColumnFamily<Object, Object> cfs;
    private AbstractSerializer colSer;
    private AbstractSerializer kser;
    private final String cfName;

    AstyanaxOperation(String columnName)
    {
        this.cfName = columnName;
    }

    @Override
    public void serlizers(AbstractSerializer kser, AbstractSerializer colser, AbstractSerializer valser)
    {
        this.kser = kser;
        this.valser = valser;
        this.cfs = new ColumnFamily(cfName, kser, colser);
        this.colSer = colser;
    }

    @Override
    public ResponseData put(Object key, Object colName, Object value) throws OperationException
    {
        MutationBatch m = AstyanaxConnection.instance.keyspace().prepareMutationBatch();
        m.withRow(cfs, key).putColumn(colName, value, valser, null);
        try
        {
            return new ResponseData("", 0, m.execute());
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
            ByteBuffer bbName = sp.columnAsByteBuffer(colName);
            ByteBuffer bbKey = sp.keyAsByteBuffer(key);
            ColumnFamily columnFamily = new ColumnFamily(cfName, ByteBufferSerializer.get(), ByteBufferSerializer.get());
            ColumnMutation mutation = AstyanaxConnection.instance.keyspace().prepareColumnMutation(columnFamily, bbKey, bbName);
            return new ResponseData("", 0, mutation.putValue(value, null).execute());
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
            cf.putColumn(entry.getKey(), entry.getValue(), valser, null);
        try
        {
            return new ResponseData("", 0, m.execute());
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
            bytes = opResult.getResult().getByteArrayValue().length;
            String value = SystemUtils.convertToString(valser, opResult.getResult().getByteArrayValue());
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

        return new ResponseData(response.toString(), bytes, opResult);
    }

    @Override
    public ResponseData getCompsote(String key, String colName) throws OperationException
    {
        StringBuffer response = new StringBuffer();
        int bytes = 0;
        OperationResult<Column<Object>> opResult = null;
        try
        {
            SerializerPackage sp = AstyanaxConnection.instance.keyspace().getSerializerPackage(cfName, false);
            ByteBuffer bbName = sp.columnAsByteBuffer(colName);
            ByteBuffer bbKey = sp.keyAsByteBuffer(key);
            ColumnFamily columnFamily = new ColumnFamily(cfName, ByteBufferSerializer.get(), ByteBufferSerializer.get());
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(columnFamily).getKey(bbKey).getColumn(bbName).execute();
            bytes = opResult.getResult().getByteArrayValue().length;
            String value = SystemUtils.convertToString(valser, opResult.getResult().getByteArrayValue());
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
        return new ResponseData(response.toString(), bytes, opResult);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException
    {
        int bytes = 0;
        OperationResult<ColumnList<Object>> opResult = null;
        StringBuffer response = new StringBuffer().append("\n");
        try
        {
            RangeBuilder rb = new RangeBuilder().setStart(startColumn, colSer).setEnd(endColumn, colSer).setMaxSize(count);
            if (reversed)
                rb.setReversed();
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(cfs).getKey(rKey).withColumnRange(rb.build()).execute();
            Iterator<?> it = opResult.getResult().iterator();
            while (it.hasNext())
            {
                Column<?> col = (Column<?>) it.next();
                String key = col.getName().toString();
                bytes += key.getBytes().length;
                String value = SystemUtils.convertToString(valser, col.getByteArrayValue());
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
        return new ResponseData(response.toString(), bytes, opResult);
    }

    @Override
    public ResponseData delete(Object rkey, Object colName) throws OperationException
    {
        OperationResult<Void> opResult = null;
        try
        {
            opResult = AstyanaxConnection.instance.keyspace().prepareColumnMutation(cfs, rkey, colName).deleteColumn().execute();
        }
        catch (ConnectionException e)
        {
            throw new OperationException(e);
        }

        return new ResponseData(null, 0, opResult);
    }
}
