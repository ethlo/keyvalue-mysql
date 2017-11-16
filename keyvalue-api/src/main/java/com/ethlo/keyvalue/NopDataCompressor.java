package com.ethlo.keyvalue;

public class NopDataCompressor implements DataCompressor
{
    @Override
    public byte[] compress(byte[] uncompressed)
    {
        return uncompressed;
    }

    @Override
    public byte[] decompress(byte[] compressed)
    {
        return compressed;
    }
}
