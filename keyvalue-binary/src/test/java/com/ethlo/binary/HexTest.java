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
