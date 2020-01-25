package com.ethlo.keyvalue.compression;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.UnsupportedOptionsException;
import org.tukaani.xz.XZ;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import com.ethlo.binary.StreamUtil;

public class Lzma2DataCompressor extends AbstractDataCompressor
{
    private static final LZMA2Options options = new LZMA2Options();

    static
    {
        try
        {
            options.setMode(LZMA2Options.MODE_FAST);
            options.setMatchFinder(LZMA2Options.MF_HC4);
            options.setDictSize(LZMA2Options.DICT_SIZE_MIN);
            options.setNiceLen(100);
        }
        catch (UnsupportedOptionsException exc)
        {
            throw new IllegalStateException(exc);
        }
    }

    public Lzma2DataCompressor()
    {
        super(CompressionType.LZMA2.getId());
    }

    @Override
    protected void handleCompression(final byte[] uncompressed, final OutputStream out) throws IOException
    {
        final OutputStream compOut = new XZOutputStream(out, options, XZ.CHECK_CRC64);
        compOut.write(uncompressed);
        compOut.close();
    }

    @Override
    public void handleDecompression(final int uncompressedLength, int sourceOffset, final byte[] compressed, final OutputStream out) throws IOException
    {
        final ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
        bin.skip(sourceOffset);
        StreamUtil.copy(new XZInputStream(bin), out);
    }
}
