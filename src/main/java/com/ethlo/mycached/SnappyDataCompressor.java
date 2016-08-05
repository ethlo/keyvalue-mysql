package com.ethlo.mycached;

import com.ethlo.keyvalue.DataCompressor;

public class SnappyDataCompressor implements DataCompressor
{
    @Override
    public byte[] compress(byte[] uncompressed)
    {
        return CompressionUtil.compress(uncompressed);
    }

    @Override
    public byte[] decompress(byte[] compressed)
    {
        return CompressionUtil.uncompress(compressed);
    }
}
