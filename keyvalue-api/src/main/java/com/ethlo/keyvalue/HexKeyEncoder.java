package com.ethlo.keyvalue;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

public class HexKeyEncoder implements KeyEncoder
{
    private final HexBinaryAdapter ha = new HexBinaryAdapter();

    @Override
    public String toString(byte[] key)
    {
        return ha.marshal(key);
    }

    @Override
    public byte[] fromString(String key)
    {
        return ha.unmarshal(key);
    }
}
