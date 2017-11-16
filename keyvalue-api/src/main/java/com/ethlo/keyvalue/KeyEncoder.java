package com.ethlo.keyvalue;

public interface KeyEncoder
{
    String toString(byte[] key);
    
    byte[] fromString(String key);
}
