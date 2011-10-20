package com.netflix.jmeter.properties;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.testbeans.TestBean;

public class SchemaProperties extends ConfigTestElement implements TestBean
{
    private static final long serialVersionUID = 468255622613306730L;
    private static final String strategy_options = "strategy_options";
    private static final String comparator_type = "comparator_type";
    private static final String key_validation_class = "key_validation_class";
    private static final String default_validation_class = "default_validation_class";
    private static final String validator = "validator";
    private static final String rows_cached = "rows_cached";
    private static final String row_cache_provider = "row_cache_provider";
    private static final String read_repair_chance = "read_repair_chance";
    private static final String memtable_flush_after = "memtable_flush_after";
    private static final String memtable_throughput = "memtable_throughput";

    public SchemaProperties()
    {
        Properties.instance.schema = this;
    }

    public String getStrategy_options()
    {
        return getPropertyAsString(strategy_options);
    }

    public void setStrategy_options(String val)
    {
        setProperty(strategy_options, val);
    }

    public String getKey_validation_class()
    {
        return getPropertyAsString(key_validation_class);
    }

    public void setKey_validation_class(String val)
    {
        setProperty(key_validation_class, val);
    }

    public String getValidator()
    {
        return getPropertyAsString(validator);
    }

    public void setValidator(String val)
    {
        setProperty(validator, val);
    }

    public String getDefault_validation_class()
    {
        return getPropertyAsString(default_validation_class);
    }

    public void setDefault_validation_class(String val)
    {
        setProperty(default_validation_class, val);
    }

    public String getRows_cached()
    {
        return getPropertyAsString(rows_cached);
    }

    public void setRows_cached(String val)
    {
        setProperty(rows_cached, val);
    }

    public String getRow_cache_provider()
    {
        return getPropertyAsString(row_cache_provider);
    }

    public void setRow_cache_provider(String val)
    {
        setProperty(row_cache_provider, val);
    }

    public String getMemtable_flush_after()
    {
        return getPropertyAsString(memtable_flush_after);
    }

    public void setMemtable_flush_after(String val)
    {
        setProperty(memtable_flush_after, val);
    }

    public String getMemtable_throughput()
    {
        return getPropertyAsString(memtable_throughput);
    }

    public void setMemtable_throughput(String val)
    {
        setProperty(memtable_throughput, val);
    }

    public String getRead_repair_chance()
    {
        return getPropertyAsString(read_repair_chance);
    }

    public void setRead_repair_chance(String val)
    {
        setProperty(read_repair_chance, val);
    }

    public String getComparator_type()
    {
        return getPropertyAsString(comparator_type);
    }

    public void setComparator_type(String val)
    {
        setProperty(comparator_type, val);
    }
}