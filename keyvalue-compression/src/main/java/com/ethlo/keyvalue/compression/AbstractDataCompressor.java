package com.ethlo.keyvalue.compression;

/*-
 * #%L
 * keyvalue-compression
 * %%
 * Copyright (C) 2013 - 2020 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import com.ethlo.binary.UnsignedUtil;

/**
 * Base compressor that prefixes the compressed value with:
 * * 1 byte for type of compression
 * * 1 byte for whether the length is embedded
 * * Optionally 4 bytes denoting the length of the uncompressed data (for more efficient decompression buffers)
 */
public abstract class AbstractDataCompressor implements DataCompressor
{
    public static final int HEADER_LENGTH = 4;

    private final int type;

    public AbstractDataCompressor(final int type)
    {
        this.type = type;
    }

    @Override
    public int getType()
    {
        return type;
    }

    @Override
    public final byte[] compress(final byte[] uncompressed)
    {
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(HEADER_LENGTH + uncompressed.length))
        {
            outputStream.write(UnsignedUtil.encodeUnsigned(type, 1));
            outputStream.write(UnsignedUtil.encodeUnsigned(uncompressed.length, 3));
            handleCompression(uncompressed, outputStream);
            return outputStream.toByteArray();
        }
        catch (IOException exc)
        {
            throw new UncheckedIOException(exc);
        }
    }

    @Override
    public final byte[] decompress(final byte[] compressed)
    {
        final int type = (int) UnsignedUtil.decodeUnsigned(compressed, 0, 1);
        if (type != getType())
        {
            throw new UncheckedIOException(new IOException("Unexpected type. Data claims " + type + ", but this compressor supports only " + getType()));
        }
        final int uncompressedLength = Math.toIntExact(UnsignedUtil.decodeUnsigned(compressed, 1, 3));
        final int bufferSize = HEADER_LENGTH + uncompressedLength;
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bufferSize))
        {
            handleDecompression(uncompressedLength, HEADER_LENGTH, compressed, outputStream);
            return outputStream.toByteArray();
        }
        catch (IOException exc)
        {
            throw new UncheckedIOException(exc);
        }
    }

    protected abstract void handleCompression(byte[] uncompressed, OutputStream out) throws IOException;

    public abstract void handleDecompression(int uncompressedLength, int inputOffset, byte[] compressed, OutputStream out) throws IOException;
}
