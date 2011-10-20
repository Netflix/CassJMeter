package com.netflix.jmeter.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.google.common.collect.Lists;

public class YamlUpdater
{
    private Yaml yaml;
    private Map map;
    private File yamlFile;

    public YamlUpdater(String location) throws FileNotFoundException
    {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        yaml = new Yaml(options);
        yamlFile = new File(location);
        map = (Map) yaml.load(new FileInputStream(yamlFile));
    }

    public void update(String key, Object value)
    {
        map.put(key, value);
    }

    public void setSeeds(Set<String> seeds)
    {
        List<?> seedp = (List) map.get("seed_provider");
        
        Map m = (Map) seedp.get(0);
        m.put("class_name", "org.apache.cassandra.locator.SimpleSeedProvider");
        
        List lst = Lists.newArrayList();
        Map map = new HashMap();
        map.put("seeds", StringUtils.join(seeds, ","));
        lst.add(map);
        m.put("parameters", lst);
    }

    public void encriptionOption(String string, String internode_encryption)
    {
        Map m = (Map) map.get("encryption_options");
        m.put(string, internode_encryption);
    }
    
    public void dump() throws IOException
    {
        yaml.dump(map, new FileWriter(yamlFile));
    }
}
