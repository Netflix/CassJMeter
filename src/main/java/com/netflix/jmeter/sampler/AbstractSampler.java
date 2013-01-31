package com.netflix.jmeter.sampler;

import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;

import com.google.common.collect.Maps;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.AsciiSerializer;
import com.netflix.astyanax.serializers.BigIntegerSerializer;
import com.netflix.astyanax.serializers.BooleanSerializer;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.CharSerializer;
import com.netflix.astyanax.serializers.DateSerializer;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.FloatSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.ShortSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.jmeter.utils.Hex;
import com.netflix.jmeter.utils.SystemUtils;

public abstract class AbstractSampler extends org.apache.jmeter.samplers.AbstractSampler
{
    private static final long serialVersionUID = -8637635942486594464L;
    public static final String KEY = "KEY";
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String KEY_SERIALIZER_TYPE = "KEY_SERIALIZER_TYPE";
    public static final String COLUMN_SERIALIZER_TYPE = "COLUMN_SERIALIZER_TYPE";
    public static final String VALUE_SERIALIZER_TYPE = "VALUE_SERIALIZER_TYPE";
    public static final String COLUMN_FAMILY = "COLUMN_FAMILY";
    public static final String IS_COUNTER = "IS_COUNTER";

    @SuppressWarnings("rawtypes")
    public static Map<String, AbstractSerializer> serializers = Maps.newHashMap();
    static
    {
        serializers.put("StringSerializer", StringSerializer.get());
        serializers.put("IntegerSerializer", IntegerSerializer.get());
        serializers.put("LongSerializer", LongSerializer.get());
        serializers.put("BooleanSerializer", BooleanSerializer.get());
        serializers.put("DoubleSerializer", DoubleSerializer.get());
        serializers.put("DateSerializer", DateSerializer.get());
        serializers.put("FloatSerializer", FloatSerializer.get());
        serializers.put("ShortSerializer", ShortSerializer.get());
        serializers.put("UUIDSerializer", UUIDSerializer.get());
        serializers.put("BigIntegerSerializer", BigIntegerSerializer.get());
        serializers.put("CharSerializer", CharSerializer.get());
        serializers.put("AsciiSerializer", AsciiSerializer.get());
        serializers.put("BytesArraySerializer", BytesArraySerializer.get());
    }

    public Object convert(String text, String kSerializerType)
    {
        if (kSerializerType.equals("StringSerializer"))
        {
            return text;
        }
        else if (kSerializerType.equals("IntegerSerializer"))
        {
            return Integer.parseInt(text);
        }
        else if (kSerializerType.equals("LongSerializer"))
        {
            return Long.parseLong(text);
        }
        else if (kSerializerType.equals("BooleanSerializer"))
        {
            return Boolean.parseBoolean(text);
        }
        else if (kSerializerType.equals("DoubleSerializer"))
        {
            return Double.parseDouble(text);
        }
        else if (kSerializerType.equals("BooleanSerializer"))
        {
            return Boolean.parseBoolean(text);
        }
        else if (kSerializerType.equals("DateSerializer"))
        {
            return Date.parse(text);
        }
        else if (kSerializerType.equals("FloatSerializer"))
        {
            return Float.parseFloat(text);
        }
        else if (kSerializerType.equals("ShortSerializer"))
        {
            return Short.parseShort(text);
        }
        else if (kSerializerType.equals("UUIDSerializer"))
        {
            return UUID.fromString(text);
        }
        else if (kSerializerType.equals("BigIntegerSerializer"))
        {
            return new BigInteger(text);
        }
        else if (kSerializerType.equals("CharSerializer"))
        {
            // TODO fix it.
            return text;
        }
        else if (kSerializerType.equals("AsciiSerializer"))
        {
            return text;
        }
        else if (kSerializerType.equals("BytesArraySerializer"))
        {
            return Hex.hexToBytes(text);
        }
        return serializers.get(kSerializerType).fromString(text);
    }

    public void setColumnName(String text)
    {
        setProperty(COLUMN_NAME, text);
    }

    public Object getColumnName()
    {
        String text = getProperty(COLUMN_NAME).getStringValue();
        return convert(text, getCSerializerType());
    }

    public void setKSerializerType(String text)
    {
        setProperty(KEY_SERIALIZER_TYPE, text);
    }

    public void setCSerializerType(String text)
    {
        setProperty(COLUMN_SERIALIZER_TYPE, text);
    }

    public void setVSerializerType(String text)
    {
        setProperty(VALUE_SERIALIZER_TYPE, text);
    }

    public void setKey(String text)
    {
        setProperty(KEY, text);
    }

    public Object getKey()
    {
        String text = getProperty(KEY).getStringValue();
        return convert(text, getKSerializerType());
    }

    public void setColumnFamily(String text)
    {
        setProperty(COLUMN_FAMILY, text);
    }

    public String getColumnFamily()
    {
        return getProperty(COLUMN_FAMILY).getStringValue();
    }

    public String getKSerializerType()
    {
        return getProperty(KEY_SERIALIZER_TYPE).getStringValue();
    }

    public String getCSerializerType()
    {
        return getProperty(COLUMN_SERIALIZER_TYPE).getStringValue();
    }

    public String getVSerializerType()
    {
        return getProperty(VALUE_SERIALIZER_TYPE).getStringValue();
    }

    public static AbstractSerializer serialier(String text)
    {
        return serializers.get(text);
    }

    public static Set<String> getSerializerNames()
    {
        return serializers.keySet();
    }

    public SampleResult sample(Entry e)
    {
        SampleResult sr = new SampleResult();
        sr.setSampleLabel(getName());
        sr.sampleStart();
        sr.setDataType(SampleResult.TEXT);
        long start = sr.currentTimeInMillis();
        String message = "ERROR: UNKNOWN";
        ResponseData response = null;
        try
        {
            response = execute();
            sr.setBytes(response.size);
            message = response.responseRecived;
            sr.setSuccessful(true);
            sr.setResponseCodeOK();
            sr.setResponseHeaders(response.requestSent);
        }
        catch (Exception ex)
        {
            message = SystemUtils.getStackTrace(ex);
            sr.setSuccessful(false);
        }
        finally
        {
            sr.setResponseData(message);
            long latency = System.currentTimeMillis() - start;
            sr.sampleEnd();
            sr.setLatency(latency);
            /*
             * A6x reports the latency via thrift. the following logic will
             * figure out the connection pooling time... the following applies
             * only for A6x clients. rest will set the latency = 0
             */
            if (response != null && response.latency_in_ms != 0)
                sr.setIdleTime(latency - response.latency_in_ms);
        }
        return sr;
    }

    public abstract ResponseData execute() throws Exception;

    public static class ResponseData
    {
        protected static final String EXECUTED_ON = "Executed on: ";
        protected static final String ROW_KEY = "Row Key: ";
        protected static final String COLUMN_NAME = "Column Name: ";
        protected static final String COLUMN_VALUE = "Column Value: ";

        public final String responseRecived;
        public final String requestSent;
        public final int size;
        public final long latency_in_ms;

        protected ResponseData(String response, int size, String requestSent, long latency)
        {
            this.responseRecived = response;
            this.size = size;
            this.requestSent = requestSent;
            this.latency_in_ms = latency;
        }

        public ResponseData(String response, int size, String request)
        {
            this(response, size, EXECUTED_ON + request, 0);
        }

        public ResponseData(String response, int size, String host, long latency, Object key, Object cn, Object value)
        {
            this(response, size, constructRequest(host, key, cn, value), latency);
        }

        public ResponseData(String response, int size, String host, long latency, Object key, Map<?, ?> kv)
        {
            this(response, size, constructRequest(host, key, kv), latency);
        }
        
        private static String constructRequest(String host, Object key, Object cn, Object value)
        {
            StringBuffer buff = new StringBuffer();
            appendHostAndRowKey(buff, host, key);
            appendKeyValue(buff, cn, value);
            return buff.toString();
        }

        private static String constructRequest(String host, Object key, Map<?, ?> nv)
        {
            StringBuffer buff = new StringBuffer();
            appendHostAndRowKey(buff, host, key);
            for (Map.Entry<?, ?> entry : nv.entrySet())
                appendKeyValue(buff, entry.getKey(), entry.getValue());
            return buff.toString();
        }
        
        private static void appendHostAndRowKey(StringBuffer buff, String host, Object key)
        {
            buff.append(EXECUTED_ON).append(host).append(SystemUtils.NEW_LINE);
            buff.append(ROW_KEY).append(key).append(SystemUtils.NEW_LINE);            
        }

        private static void appendKeyValue(StringBuffer buff, Object cn, Object value)
        {
            if (cn != null)
                buff.append(COLUMN_NAME).append(null == cn ? "" :cn).append(SystemUtils.NEW_LINE);
            if (value != null)
                buff.append(COLUMN_VALUE).append(null == value ? "" :value).append(SystemUtils.NEW_LINE);
        }
    }

    public void setSerializers(Operation ops)
    {
        AbstractSerializer<?> vser = serialier(getVSerializerType());
        AbstractSerializer<?> kser = serialier(getKSerializerType());
        AbstractSerializer<?> cser = serialier(getCSerializerType());
        ops.serlizers(kser, cser, vser);
    }

    public void setCounter(boolean selected)
    {
        setProperty(IS_COUNTER, selected);
    }

    public boolean isCounter()
    {
        return getPropertyAsBoolean(IS_COUNTER);
    }
}
