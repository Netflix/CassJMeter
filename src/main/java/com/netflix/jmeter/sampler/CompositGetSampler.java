package com.netflix.jmeter.sampler;

public class CompositGetSampler extends AbstractSampler
{
    private static final long serialVersionUID = -2103499609822848595L;

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstace().newOperation(getColumnFamily());
        setSerializers(ops);
        return ops.getCompsote(getProperty(KEY).getStringValue(), getProperty(COLUMN_NAME).getStringValue());
    }
}
