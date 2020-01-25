package com.ethlo.binary;

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
