package com.ethlo.binary;

/*-
 * #%L
 * keyvalue-binary
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

import org.junit.Test;

public class UnsignedUtilTest
{
    @Test
    public void encodeByte()
    {
        final short value = (short) UnsignedUtil.MAX_UNSIGNED_8BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsignedByte(value);
        assertThat(result).isEqualTo(new byte[]{-1});
    }

    @Test
    public void encodeShort()
    {
        final long value = UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsigned(value, 2);
        assertThat(result).isEqualTo(new byte[]{(byte) 0xFF, (byte) 0xFF});
    }

    @Test
    public void encode24Bit()
    {
        final long value = UnsignedUtil.MAX_UNSIGNED_24BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsigned(value, 3);
        assertThat(result).isEqualTo(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    }

    @Test
    public void encode32Bit()
    {
        final long value = UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE;
        final byte[] result = UnsignedUtil.encodeUnsigned(value, 4);
        assertThat(result).isEqualTo(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    }

    @Test
    public void decodeUnsignedInt()
    {
        final byte[] data = new byte[]{-1, -1, -1, -1};
        final long result = UnsignedUtil.decodeUnsignedInt(data, 0);
        assertThat(result).isEqualTo(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void decodeUnsignedZeroLength()
    {
        final byte[] data = new byte[]{-1};
        UnsignedUtil.decodeUnsigned(data, 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void decodeUnsignedMoreThan64BitLength()
    {
        final byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        UnsignedUtil.decodeUnsigned(data, 0, 9);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void decodeUnsignedOutOfArrayBounds()
    {
        final byte[] data = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        UnsignedUtil.decodeUnsigned(data, 5, 6);
    }

    @Test
    public void decodeUnsignedShort()
    {
        final byte[] data = new byte[]{-1, -1};
        final long result = UnsignedUtil.decodeUnsignedShort(data, 0);
        assertThat(result).isEqualTo(UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE);
    }

    @Test
    public void encodeUnsigned()
    {
        for (int length = 1; length <= 8; length++)
        {
            final byte[] value = UnsignedUtil.encodeUnsigned(255, length);
            assertThat(value.length).isEqualTo(length);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void getMaxSizeZeroBytes()
    {
        UnsignedUtil.getMaxSize(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getMaxSizeMoreThan8Bytes()
    {
        UnsignedUtil.getMaxSize(9);
    }

    @Test
    public void getMaxSize()
    {
        assertThat(UnsignedUtil.getMaxSize(1)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_8BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(2)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(3)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_24BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(4)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(5)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_40BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(6)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_48BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(7)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_56BIT_INT_VALUE);
        assertThat(UnsignedUtil.getMaxSize(8)).isEqualTo(UnsignedUtil.MAX_UNSIGNED_63BIT_INT_VALUE);
    }

    @Test
    public void encodeUnsignedByte()
    {
        final short value = 255;
        final byte[] data = UnsignedUtil.encodeUnsignedByte(value);
        assertThat(data.length).isEqualTo(1);
        assertThat(UnsignedUtil.decodeUnsignedByte(data, 0)).isEqualTo(value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void encodeTooLargeValueForBits()
    {
        final int value = 70_000;
        UnsignedUtil.encodeUnsignedShort(value);
    }

    @Test
    public void encodeUnsignedShort()
    {
        final int value = 255;
        final byte[] data = UnsignedUtil.encodeUnsignedShort(value);
        assertThat(data.length).isEqualTo(2);
        assertThat(UnsignedUtil.decodeUnsignedShort(data, 0)).isEqualTo(value);
    }

    @Test
    public void encodeUnsignedIntWithReuiredNumberOfBytes()
    {
        final long value = 998281928292021093L;
        final int bytesRequired = UnsignedUtil.getRequiredBytesForUnsigned(value);
        assertThat(bytesRequired).isEqualTo(8);
        final byte[] data = UnsignedUtil.encodeUnsigned(value, bytesRequired);
        assertThat(UnsignedUtil.decodeUnsigned(data, 0, bytesRequired)).isEqualTo(value);
    }

    @Test
    public void decodeUnsignedByte()
    {
        final short value = 255;
        final byte[] data = UnsignedUtil.encodeUnsignedByte(value);
        assertThat(data.length).isEqualTo(1);
        assertThat(UnsignedUtil.decodeUnsignedByte(data, 0)).isEqualTo(value);
    }

    @Test
    public void decodeUnsigned()
    {
        final long value = 998281928292021093L;
        final int bytesRequired = UnsignedUtil.getRequiredBytesForUnsigned(value);
        assertThat(bytesRequired).isEqualTo(8);
        final byte[] data = UnsignedUtil.encodeUnsigned(value, bytesRequired);
        assertThat(UnsignedUtil.decodeUnsigned(data, 0, bytesRequired)).isEqualTo(value);
    }

    @Test
    public void getRequiredBytesForUnsigned()
    {
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_8BIT_INT_VALUE, 1);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_16BIT_INT_VALUE, 2);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_24BIT_INT_VALUE, 3);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE, 4);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_40BIT_INT_VALUE, 5);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_48BIT_INT_VALUE, 6);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_56BIT_INT_VALUE, 7);
        checkAtBoundary(UnsignedUtil.MAX_UNSIGNED_63BIT_INT_VALUE, 8);
    }

    private void checkAtBoundary(final long value, final int byteCountAssert)
    {
        assertThat(UnsignedUtil.getRequiredBytesForUnsigned(value)).isEqualTo(byteCountAssert);
        assertThat(UnsignedUtil.getRequiredBytesForUnsigned(value - 1)).isEqualTo(byteCountAssert);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getRequiredBytesForUnsignedNegative()
    {
        final long value = -1;
        UnsignedUtil.getRequiredBytesForUnsigned(value);
    }

    @Test
    public void encodeUnsignedInt()
    {
        final byte[] data = UnsignedUtil.encodeUnsignedInt(UnsignedUtil.MAX_UNSIGNED_32BIT_INT_VALUE);
        assertThat(data).isEqualTo(new byte[]{-1, -1, -1, -1});
    }
}
