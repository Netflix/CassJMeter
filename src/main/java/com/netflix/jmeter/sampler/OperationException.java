package com.netflix.jmeter.sampler;

public class OperationException extends Exception
{
    private static final long serialVersionUID = 1L;

    public OperationException(Exception ex)
    {
        super(ex);
    }
}
