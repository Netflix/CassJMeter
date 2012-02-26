package com.netflix.jmeter.utils;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.Hex;

import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;

public class SystemUtils
{
    public static final String NEW_LINE = System.getProperty("line.separator");

    public static String getStackTrace(Throwable aThrowable)
    {
        final StringBuilder result = new StringBuilder("ERROR: ");
        result.append(aThrowable.toString());
        result.append(NEW_LINE);
        // add each element of the stack trace
        for (StackTraceElement element : aThrowable.getStackTrace())
        {
            result.append(element.toString());
            result.append(NEW_LINE);
        }
        return result.toString();
    }
    
    
    public static String convertToString(AbstractSerializer ser, ByteBuffer byteBuffer)
    {
        String value;
        if (ser instanceof BytesArraySerializer)
            value = Hex.bytesToHex(byteBuffer.array());
        else
            value = ser.fromByteBuffer(byteBuffer).toString();
        return value;
    }
    
}
