package com.netflix.jmeter.sampler;

import java.util.Map;

import com.google.common.collect.Maps;

public class CompositeBatchPutSampler extends AbstractSampler
{
    private static final long serialVersionUID = 6393722552275749483L;
    public static final String NAME_AND_VALUE = "NAME_AND_VALUE";
    public static final String IS_Batch = "IS_Batch";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), isCounter());
        setSerializers(ops);
        Map<?, ?> nv = getNameValue();
        return ops.batchMutate(getKey(), nv);
    }

    public Map<?, ?> getNameValue()
    {
        Map<Object, Object> return_ = Maps.newHashMap();
        String text = getProperty(NAME_AND_VALUE).getStringValue();
        for (String str : text.split("[\\r\\n]+"))
        {
            String[] cv = str.split(":", 2);
            String cName = cv[0];
            String vName = cv[1];
            return_.put(convert(cName, getCSerializerType()), convert(vName, getVSerializerType()));
        }
        return return_;
    }
}
