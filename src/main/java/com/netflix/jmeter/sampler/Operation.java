package com.netflix.jmeter.sampler;

import java.nio.ByteBuffer;
import java.util.Map;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.jmeter.sampler.AbstractSampler.ResponseData;

public interface Operation
{
    void serlizers(AbstractSerializer kser, AbstractSerializer colser, AbstractSerializer valser);

    ResponseData put(Object key, Object colName, Object value) throws OperationException;

    ResponseData batchMutate(Object key, Map<?, ?> nv) throws OperationException;

    ResponseData get(Object rkey, Object colName) throws OperationException;

    ResponseData rangeSlice(Object rKey, Object startColumn, Object endColumn, boolean reversed, int count) throws OperationException;

    ResponseData putComposite(String key, String colName, ByteBuffer vbb) throws OperationException;

    ResponseData batchCompositeMutate(String key, Map<String, ByteBuffer> nv) throws OperationException;

    ResponseData getCompsote(String stringValue, String stringValue2) throws OperationException;
}