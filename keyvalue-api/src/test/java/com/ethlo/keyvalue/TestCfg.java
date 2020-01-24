package com.ethlo.keyvalue;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ethlo.keyvalue.hashmap.HashmapKeyValueDbManager;

@Configuration
public class TestCfg
{
    @Bean
    public KeyValueDbManager keyValueDbManager()
    {
        return new HashmapKeyValueDbManager();
    }
}
