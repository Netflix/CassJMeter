package com.netflix.jmeter.sampler;

import java.util.Map;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.jmeter.sampler.AbstractCassandraSampler.ResponseData;

public interface Operation
{
    public void serlizers(AbstractSerializer kser, AbstractSerializer colser, AbstractSerializer valser);

    public ResponseData put(Object key, Object colName, Object value) throws OperationException;

    public ResponseData batchMutate(Object key, Map<?, ?> nv) throws OperationException;

    public ResponseData get(Object rkey, Object colName) throws OperationException;

    public ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException;
}