package com.netflix.jmeter.sampler;

import java.nio.ByteBuffer;

public class CompsitePutSampler extends AbstractSampler
{
    private static final long serialVersionUID = 6393722552275749483L;
    public static final String VALUE = "VALUE";
    public static final String IS_Batch = "IS_Batch";
    public static final String IS_Commit = "IS_Commit";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), isCounter());
        setSerializers(ops);
        return ops.putComposite(getProperty(KEY).getStringValue(), getProperty(COLUMN_NAME).getStringValue(), getValue());
    }

    public ByteBuffer getValue()
    {
        String text = getProperty(VALUE).getStringValue();
        return serialier(getVSerializerType()).fromString(text);
    }

    public void setValue(String text)
    {
        setProperty(VALUE, text);
    }
}
