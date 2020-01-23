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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.ethlo.binary.UnsignedUtil;

public class Lz4DataCompressorTest
{
    private final byte[] sampledata = "hello there! !/(#/649870ÆØ34".getBytes(StandardCharsets.UTF_8);
    private final DataCompressor dataCompressor = new Lz4DataCompressor();

    @Test
    public void compressAndDecompress()
    {
        final byte[] compressed = dataCompressor.compress(sampledata);
        assertThat(compressed).isNotNull();
        assertThat(compressed).hasSizeGreaterThan(4 + 1);
        final int claimedUncompressedLength = Math.toIntExact(UnsignedUtil.getUnsignedInt(compressed, 0, 4));
        assertThat(claimedUncompressedLength).isEqualTo(sampledata.length);
        final byte[] decompressed = dataCompressor.decompress(compressed);
        assertThat(decompressed).isEqualTo(sampledata);
    }
}
