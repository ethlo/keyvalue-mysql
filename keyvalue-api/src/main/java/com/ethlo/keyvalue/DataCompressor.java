package com.ethlo.keyvalue;

public interface DataCompressor
{
    byte[] compress(byte[] uncompressed);
    
    byte[] decompress(byte[] compressed);
}
