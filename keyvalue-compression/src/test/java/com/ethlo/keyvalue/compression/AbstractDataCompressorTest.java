package com.ethlo.keyvalue.compression;

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
            " magna, non rutrum est rutrum sed. Fusce facilisis felis purus, vestibulum consequat orci tincidunt ut." +
            " Aliquam sit amet pharetra augue, non molestie velit. Ut mollis sed lectus a tristique. Aenean vitae orci et " +
            "eros euismod rhoncus vel nec tortor. Nam faucibus, orci a pretium dictum, dolor nisi laoreet tellus, " +
            "nec tempor est leo quis nulla. Duis in felis sapien. Donec luctus leo ac sollicitudin ornare. Maecenas facilisis" +
            " neque id sem facilisis, id tincidunt velit sagittis. Suspendisse lacinia urna ornare laoreet interdum.")
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
        final int claimedUncompressedLength = Math.toIntExact(UnsignedUtil.getUnsignedInt(compressed, 1, 3));
        assertThat(claimedUncompressedLength).isEqualTo(sampledata.length);
        final byte[] decompressed = dataCompressor.decompress(compressed);
        assertThat(decompressed).isEqualTo(sampledata);
    }

    @Test
    public void performanceTest()
    {
        final Chronograph chronograph = Chronograph.create();
        chronograph.start("compress");
        for (int i = 0; i < getPerformanceTestIteration(); i++)
        {
            assertThat(dataCompressor.compress(sampledata)).isNotNull();
        }
        chronograph.stop();
        logger.info(chronograph.prettyPrint());
    }

    protected abstract int getPerformanceTestIteration();
}
