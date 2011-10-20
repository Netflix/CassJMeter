package com.netflix.jmeter.connections.a6x;

import java.util.Iterator;
import java.util.Map;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.jmeter.sampler.AbstractCassandraSampler.ResponseData;
import com.netflix.jmeter.sampler.Operation;
import com.netflix.jmeter.sampler.OperationException;
import com.netflix.jmeter.utils.SystemUtils;

public class AstyanaxOperation implements Operation
{
    private AbstractSerializer valser;
    private ColumnFamily<Object, Object> cfs;

    @Override
    public void serlizers(AbstractSerializer kser, AbstractSerializer colser, AbstractSerializer valser)
    {
        this.valser = valser;
        this.cfs = new ColumnFamily(AstyanaxConnection.instance.getColumnFamilyName(), kser, colser);
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
            response.append(opResult.getResult().getValue(valser).toString());
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
    public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException
    {
        int bytes = 0;
        OperationResult<ColumnList<Object>> opResult = null;
        StringBuffer response = new StringBuffer().append("\n");
        try
        {
            opResult = AstyanaxConnection.instance.keyspace().prepareQuery(cfs).getKey(rKey).withColumnRange(startColumn, endColumn, reversed, count).execute();
            Iterator<?> it = opResult.getResult().iterator();
            while (it.hasNext())
            {
                Column<?> col = (Column<?>) it.next();
                String key = col.getName().toString();
                bytes += key.getBytes().length;
                String value = valser.fromByteBuffer(col.getByteBufferValue()).toString();
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
}
