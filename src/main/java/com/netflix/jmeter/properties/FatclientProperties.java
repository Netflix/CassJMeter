package com.netflix.jmeter.properties;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.testbeans.TestBean;

public class FatclientProperties extends ConfigTestElement implements TestBean
{
    private static final long serialVersionUID = 468255622613306730L;
    private static final String seed_provider = "seed_provider";
    private static final String dynamic_snitch = "dynamic_snitch";
    private static final String endpoint_Snitch = "endpoint_Snitch";
    private static final String rpc_timeout_in_ms = "rpc_timeout_in_ms";
    private static final String internode_encryption = "internode_encryption";

    public FatclientProperties()
    {
        Properties.instance.fatclient = this;
    }

    public String getEndpoint_Snitch()
    {
        return getPropertyAsString(endpoint_Snitch);
    }

    public void setEndpoint_Snitch(String val)
    {
        setProperty(endpoint_Snitch, val);
    }

    public String getSeed_provider()
    {
        return getPropertyAsString(seed_provider);
    }

    public void setSeed_provider(String val)
    {
        setProperty(seed_provider, val);
    }

    public String getDynamic_snitch()
    {
        return getPropertyAsString(dynamic_snitch);
    }

    public void setDynamic_snitch(String val)
    {
        setProperty(dynamic_snitch, val);
    }

    public String getRpc_timeout_in_ms()
    {
        return getPropertyAsString(rpc_timeout_in_ms);
    }

    public void setRpc_timeout_in_ms(String val)
    {
        setProperty(rpc_timeout_in_ms, val);
    }

    public String getInternode_encryption()
    {
        return getPropertyAsString(internode_encryption);
    }

    public void setInternode_encryption(String val)
    {
        setProperty(internode_encryption, val);
    }
}