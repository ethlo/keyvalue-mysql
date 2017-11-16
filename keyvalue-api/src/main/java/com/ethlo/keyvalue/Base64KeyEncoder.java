package com.ethlo.keyvalue;

import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

public class Base64KeyEncoder implements KeyEncoder
{
    private final Encoder encoder = Base64.getEncoder();
    private final Decoder decoder = Base64.getDecoder();
    
    @Override
    public String toString(byte[] key)
    {
        return encoder.encodeToString(key);
    }

    @Override
    public byte[] fromString(String key)
    {
        return decoder.decode(key);
    }
}
