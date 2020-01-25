package com.ethlo.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class HexTest
{
    final byte[] binary = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
    final String hex = "000102030405060708090a0b0c0d0e0f10111213";

    @Test
    public void testEncode()
    {
        final String hex = Hex.encode(binary);
        assertThat(hex).isEqualTo(hex);
    }

    @Test
    public void testDecode()
    {
        final byte[] data = Hex.decode(hex);
        assertThat(data).isEqualTo(binary);
    }
}