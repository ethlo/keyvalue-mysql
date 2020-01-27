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

@SuppressWarnings("ManualArrayCopy")
public class ByteArrayUtil
{
    /**
     * Set the byte in the source in the target
     *
     * @param source       The source data
     * @param target       The target data
     * @param targetOffset The offset in the target where to overwrite the bytes
     */
    public static void set(final byte[] source, final byte[] target, int targetOffset)
    {
        for (int i = 0; i < source.length; i++)
        {
            target[targetOffset + i] = source[i];
        }
    }

    /**
     * Merge two byte arrays
     *
     * @param a first byte array
     * @param b second byte array
     * @return The merged array
     */
    public static byte[] merge(byte[] a, byte[] b)
    {
        final byte[] res = new byte[a.length + b.length];
        System.arraycopy(a, 0, res, 0, a.length);
        System.arraycopy(b, 0, res, a.length, b.length);
        return res;
    }
}
