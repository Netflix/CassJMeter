package com.netflix.jmeter.sampler;

public class GetRangeSliceSampler extends AbstractSampler
{
    private static final long serialVersionUID = -8566773644299382213L;
    public static final String START_COLUMN_NAME = "START_COLUMN_NAME";
    public static final String END_COLUMN_NAME = "END_COLUMN_NAME";
    public static final String IS_REVERSE = "IS_REVERSE";
    public static final String COUNT = "COUNT";

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstance().newOperation(getColumnFamily(), false);
        setSerializers(ops);
        return ops.rangeSlice(getKey(), getStartName(), getEndName(), isReverse(), getCount());
    }

    public void setStartName(String text)
    {
        setProperty(START_COLUMN_NAME, text);
    }

    public void setEndName(String text)
    {
        setProperty(END_COLUMN_NAME, text);
    }

    public Object getStartName()
    {
        String text = getProperty(START_COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType());
    }

    public Object getEndName()
    {
        String text = getProperty(END_COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType());
    }

    public boolean isReverse()
    {
        return getPropertyAsBoolean(IS_REVERSE);
    }

    public void setReverse(boolean isReverse)
    {
        setProperty(IS_REVERSE, isReverse);
    }

    public void setCount(String text)
    {
        setProperty(COUNT, text);
    }

    public int getCount()
    {
        return getProperty(COUNT).getIntValue();
    }
}
