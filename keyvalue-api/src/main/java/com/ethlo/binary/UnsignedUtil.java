package com.ethlo.binary;

/*-
 * #%L
 * Key/Value API
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

public class UnsignedUtil
{
    public static final long MAX_UNSIGNED_48BIT_INT_VALUE = (long) Math.pow(2.0D, 48.0D) - 1L;
    public static final long MAX_UNSIGNED_40BIT_INT_VALUE = (long) Math.pow(2.0D, 40.0D) - 1L;
    public static final long MAX_UNSIGNED_32BIT_INT_VALUE = 4_294_967_295L;
    public static final long MAX_UNSIGNED_24BIT_INT_VALUE = 16_777_215L;
    public static final long MAX_UNSIGNED_16BIT_INT_VALUE = 65_535L;

    public static long getUnsignedInt(byte[] b, int offset, int length)
    {
        if (offset + length > b.length)
        {
            throw new ArrayIndexOutOfBoundsException("Cannot access bytes " + offset + "-" + (offset + length - 1) + " as array length is " + b.length);
        }
        else
        {
            switch (length)
            {
                case 1:
                    return 255 & b[offset];
                case 2:
                    return (char) ((255 & b[offset]) << 8 | 255 & b[offset + 1]);
                case 3:
                    return (255 & b[offset]) << 16 | (255 & b[offset + 1]) << 8 | 255 & b[offset + 2];
                case 4:
                    int firstByte = 255 & b[offset];
                    int secondByte = 255 & b[offset + 1];
                    int thirdByte = 255 & b[offset + 2];
                    int fourthByte = 255 & b[offset + 3];
                    return (long) (firstByte << 24 | secondByte << 16 | thirdByte << 8 | fourthByte) & 4294967295L;
                case 5:
                    return handle40bit(b, offset);
                case 6:
                    return handle48bit(b, offset);
                default:
                    throw new IllegalArgumentException("Cannot convert " + length + " bytes into unsigned int");
            }
        }
    }

    private static long handle40bit(byte[] b, int offset)
    {
        int firstByte = 255 & b[offset];
        int secondByte = 255 & b[offset + 1];
        int thirdByte = 255 & b[offset + 2];
        int fourthByte = 255 & b[offset + 3];
        int fifthByte = 255 & b[offset + 4];
        return (long) (firstByte | secondByte << 24 | thirdByte << 16 | fourthByte << 8 | fifthByte) & 4294967295L;
    }

    private static long handle48bit(byte[] b, int offset)
    {
        long firstByte = 255 & b[offset];
        long secondByte = 255 & b[offset + 1];
        long thirdByte = 255 & b[offset + 2];
        long fourthByte = 255 & b[offset + 3];
        long fifthByte = 255 & b[offset + 4];
        long sixthByte = 255 & b[offset + 5];
        return firstByte << 40 | secondByte << 32 | thirdByte << 24 | fourthByte << 16 | fifthByte << 8 | sixthByte;
    }

    public static byte[] unsignedInt(long anUnsignedInt, int bytes)
    {
        byte[] buf;
        if (bytes == 6)
        {
            assertValueFits(anUnsignedInt, bytes, MAX_UNSIGNED_48BIT_INT_VALUE);
            buf = new byte[]{(byte) ((int) ((anUnsignedInt & 280375465082880L) >> 40)), (byte) ((int) ((anUnsignedInt & 1095216660480L) >> 32)), (byte) ((int) ((anUnsignedInt & 4278190080L) >> 24)), (byte) ((int) ((anUnsignedInt & 16711680L) >> 16)), (byte) ((int) ((anUnsignedInt & 4278255360L) >> 8)), (byte) ((int) (anUnsignedInt & 255L))};
        }
        else if (bytes == 5)
        {
            assertValueFits(anUnsignedInt, bytes, MAX_UNSIGNED_40BIT_INT_VALUE);
            buf = new byte[]{(byte) ((int) ((anUnsignedInt & 4278190080L) >> 32)), (byte) ((int) ((anUnsignedInt & 4278190080L) >> 24)), (byte) ((int) ((anUnsignedInt & 16711680L) >> 16)), (byte) ((int) ((anUnsignedInt & 65280L) >> 8)), (byte) ((int) (anUnsignedInt & 255L))};
        }
        else if (bytes == 4)
        {
            assertValueFits(anUnsignedInt, bytes, MAX_UNSIGNED_32BIT_INT_VALUE);
            buf = new byte[]{(byte) ((int) ((anUnsignedInt & 4278190080L) >> 24)), (byte) ((int) ((anUnsignedInt & 16711680L) >> 16)), (byte) ((int) ((anUnsignedInt & 65280L) >> 8)), (byte) ((int) (anUnsignedInt & 255L))};
        }
        else if (bytes == 3)
        {
            assertValueFits(anUnsignedInt, bytes, MAX_UNSIGNED_24BIT_INT_VALUE);
            buf = new byte[]{(byte) ((int) ((anUnsignedInt & 16711680L) >> 16)), (byte) ((int) ((anUnsignedInt & 65280L) >> 8)), (byte) ((int) (anUnsignedInt & 255L))};
        }
        else if (bytes == 2)
        {
            assertValueFits(anUnsignedInt, bytes, MAX_UNSIGNED_16BIT_INT_VALUE);
            buf = new byte[]{(byte) ((int) ((anUnsignedInt & 65280L) >> 8)), (byte) ((int) (anUnsignedInt & 255L))};
        }
        else
        {
            if (bytes != 1)
            {
                throw new IllegalArgumentException("bytes parameter must be an integer value between 1 and 5 inclusive, got " + bytes);
            }

            assertValueFits(anUnsignedInt, bytes, 255L);
            buf = new byte[]{(byte) ((int) (anUnsignedInt & 255L))};
        }

        return buf;
    }

    private static void assertValueFits(long anUnsignedInt, int bytes, long maxValue)
    {
        if (anUnsignedInt > maxValue)
        {
            throw new IllegalArgumentException("Value too large for " + bytes + " bytes: " + anUnsignedInt + ". Maximum value allowed is " + maxValue);
        }
    }

}
