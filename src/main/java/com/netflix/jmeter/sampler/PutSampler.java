package com.netflix.jmeter.sampler;

public class PutSampler extends AbstractSampler
{
    private static final long serialVersionUID = 6393722552275749483L;
    public static final String VALUE = "VALUE";
    public static final String IS_Batch = "IS_Batch";
    public static final String IS_Commit = "IS_Commit";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), isCounter());
        setSerializers(ops);
        return ops.put(getKey(), getColumnName(), getValue());
    }

    public Object getValue()
    {
        String text = getProperty(VALUE).getStringValue();
        return convert(text, getVSerializerType());
    }

    public void setValue(String text)
    {
        setProperty(VALUE, text);
    }
}
