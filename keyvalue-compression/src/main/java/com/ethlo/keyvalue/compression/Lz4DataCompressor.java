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

import java.util.Arrays;

import javax.annotation.Nonnull;

import com.ethlo.binary.ByteArrayUtil;
import com.ethlo.binary.UnsignedUtil;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * LZ4 compressor that prefixes the compressed value with 4 bytes denoting the length of the uncompressed data
 */
public class Lz4DataCompressor implements DataCompressor
{
    public static final int UNCOMPRESSED_LENGTH_PREFIX_LENGTH = 4;
    private final LZ4Factory instance = LZ4Factory.fastestInstance();
    private final LZ4Compressor util = instance.fastCompressor();

    @Override
    public byte[] compress(@Nonnull final byte[] uncompressed)
    {
        final int maxCompressedLength = util.maxCompressedLength(uncompressed.length);
        final byte[] buffer = new byte[UNCOMPRESSED_LENGTH_PREFIX_LENGTH + maxCompressedLength];
        final int compressedLength = instance.fastCompressor().compress(uncompressed, 0, uncompressed.length, buffer, UNCOMPRESSED_LENGTH_PREFIX_LENGTH);
        ByteArrayUtil.set(buffer, 0, UnsignedUtil.unsignedInt(uncompressed.length, 4));
        return Arrays.copyOf(buffer, UNCOMPRESSED_LENGTH_PREFIX_LENGTH + compressedLength);
    }

    @Override
    public byte[] decompress(@Nonnull final byte[] compressed)
    {
        final int decompressedLength = Math.toIntExact(UnsignedUtil.getUnsignedInt(compressed, 0, UNCOMPRESSED_LENGTH_PREFIX_LENGTH));
        return instance.fastDecompressor().decompress(compressed, UNCOMPRESSED_LENGTH_PREFIX_LENGTH, decompressedLength);
    }
}
