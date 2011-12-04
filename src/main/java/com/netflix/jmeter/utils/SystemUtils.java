package com.netflix.jmeter.utils;

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
    
    
}
