package com.netflix.jmeter.sampler;

public class GetSampler extends AbstractCassandraSampler
{
    private static final long serialVersionUID = -2103499609822848595L;

    public ResponseData execute() throws OperationException
    {
        Operation ops = Connection.getInstace().newOperation();
        setSerializers(ops);
        return ops.get(getKey(), getColumnName());
    }
}
