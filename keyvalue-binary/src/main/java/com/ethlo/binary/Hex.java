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

public class Hex
{
    private Hex(){}

    public static String encode(byte[] input)
    {
        final char[] hexDigits = new char[input.length * 2];
        int index = 0;
        for (byte num : input)
        {
            hexDigits[index++] = Character.forDigit((num >> 4) & 0xF, 16);
            hexDigits[index++] = Character.forDigit((num & 0xF), 16);
        }
        return new String(hexDigits);
    }

    public static byte[] decode(String hex)
    {
        final char[] chars = hex.toCharArray();
        final byte[] result = new byte[chars.length / 2];
        int index = 0;
        for (int i = 0; i < chars.length; i += 2)
        {
            result[index++] = hexToByte(chars, i);
        }
        return result;
    }

    private static byte hexToByte(final char[] data, final int offset)
    {
        final int firstDigit = toDigit(data[offset]);
        final int secondDigit = toDigit(data[offset + 1]);
        return (byte) ((firstDigit << 4) + secondDigit);
    }

    private static int toDigit(char hexChar)
    {
        int digit = Character.digit(hexChar, 16);
        if (digit == -1)
        {
            throw new IllegalArgumentException("Invalid hex character: " + hexChar);
        }
        return digit;
    }
}
