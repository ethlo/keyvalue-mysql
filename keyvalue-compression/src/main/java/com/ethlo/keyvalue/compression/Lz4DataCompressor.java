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

import java.io.IOException;
import java.io.OutputStream;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class Lz4DataCompressor extends AbstractDataCompressor
{
    private final LZ4Factory instance = LZ4Factory.fastestJavaInstance();

    public Lz4DataCompressor()
    {
        super(CompressionType.LZ4.getId());
    }

    @Override
    protected void handleCompression(final byte[] uncompressed, OutputStream outputStream) throws IOException
    {
        final LZ4Compressor compressor = instance.fastCompressor();
        final byte[] buffer = new byte[compressor.maxCompressedLength(uncompressed.length)];
        final int length = compressor.compress(uncompressed, 0, uncompressed.length, buffer, 0);
        outputStream.write(buffer, 0, length);
    }

    @Override
    public void handleDecompression(final int uncompressedLength, int inputOffset, final byte[] compressed, OutputStream out) throws IOException
    {
        final byte[] buffer = new byte[uncompressedLength];
        instance.fastDecompressor().decompress(compressed, inputOffset, buffer, 0, buffer.length);
        out.write(buffer);
    }
}
