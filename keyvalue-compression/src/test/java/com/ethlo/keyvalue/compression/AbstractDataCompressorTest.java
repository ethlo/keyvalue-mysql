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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.binary.UnsignedUtil;
import com.ethlo.time.Chronograph;

public abstract class AbstractDataCompressorTest
{
    private final byte[] sampledata = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
            "Duis porta, nulla ac luctus imperdiet, massa quam egestas urna, ut imperdiet diam sapien sit amet " +
            "magna. Praesent sit amet sapien ante. Vestibulum convallis faucibus lectus, quis tincidunt" +
            " metus ultrices sed. Etiam dignissim tortor eget dignissim fermentum. Quisque scelerisque tortor" +
            " magna, non rutrum est rutrum sed. Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
        "Duis porta, nulla ac luctus imperdiet, massa quam egestas urna, ut imperdiet diam sapien sit amet " +
        "magna. Praesent sit amet sapien ante. Vestibulum convallis faucibus lectus, quis tincidunt" +
        " metus ultrices sed. Etiam dignissim tortor eget dignissim fermentum. Quisque scelerisque tortor" +
        " magna, non rutrum est rutrum sed. Lorem ipsum dolor sit amet, consectetur adipiscing elit. " +
        "Duis porta, nulla ac luctus imperdiet, massa quam egestas urna, ut imperdiet diam sapien sit amet " +
        "magna. Praesent sit amet sapien ante. Vestibulum convallis faucibus lectus, quis tincidunt" +
        " metus ultrices sed. Etiam dignissim tortor eget dignissim fermentum. Quisque scelerisque tortor" +
        " magna, non rutrum est rutrum sed. ")
            .getBytes(StandardCharsets.UTF_8);

    private final DataCompressor dataCompressor = create();
    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected abstract DataCompressor create();

    @Test
    public void compressAndDecompress()
    {
        final byte[] compressed = dataCompressor.compress(sampledata);
        assertThat(compressed).isNotNull();
        assertThat(compressed).hasSizeGreaterThan(10);
        final int claimedUncompressedLength = Math.toIntExact(UnsignedUtil.decodeUnsigned(compressed, 1, 3));
        assertThat(claimedUncompressedLength).isEqualTo(sampledata.length);
        final byte[] decompressed = dataCompressor.decompress(compressed);
        assertThat(decompressed).isEqualTo(sampledata);
    }

    @Test
    public void performanceTest()
    {
        final Chronograph chronograph = Chronograph.create();
        chronograph.start("compress " + getPerformanceTestIterations());
        for (int i = 0; i < getPerformanceTestIterations(); i++)
        {
            assertThat(dataCompressor.compress(sampledata)).isNotNull();
        }
        chronograph.stop();

        final byte[] compressed = dataCompressor.compress(sampledata);
        final double ratio = sampledata.length / (double) compressed.length;
        logger.info("Uncompressed: {}\nCompressed: {}\nRatio: {}", sampledata.length, compressed.length, ratio);
        chronograph.start("decompress " + getPerformanceTestIterations());
        for (int i = 0; i < getPerformanceTestIterations(); i++)
        {
            assertThat(dataCompressor.decompress(compressed)).isNotNull();
        }
        chronograph.stop();

        logger.info(chronograph.prettyPrint());
    }

    protected abstract int getPerformanceTestIterations();
}
