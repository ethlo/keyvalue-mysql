package com.ethlo.mycached;

/*-
 * #%L
 * Key/value MySQL implementation
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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.SeekableIterator;
import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.google.common.base.Function;

public class SmokeTest extends AbstractTest
{
    final ByteArrayKey keyBytes0 = new ByteArrayKey(new byte[]{0, 0});
    final ByteArrayKey keyBytes1 = new ByteArrayKey(new byte[]{1, 0});
    final ByteArrayKey keyBytes2 = new ByteArrayKey(new byte[]{1, 1});
    final ByteArrayKey keyBytes3 = new ByteArrayKey(new byte[]{1, 2});
    final ByteArrayKey keyBytes4 = new ByteArrayKey(new byte[]{2, 0});

    @Test
    public void testGetAll()
    {
        final Map<ByteArrayKey, byte[]> data = createFiveEntries();
        mutatingKeyValueDb.putAll(data);

        final Set<ByteArrayKey> keys = new TreeSet<>();
        keys.add(keyBytes0);
        keys.add(keyBytes1);
        keys.add(keyBytes2);

        final Map<ByteArrayKey, byte[]> result = mutatingKeyValueDb.getAll(keys);
        assertThat(result).hasSize(3);
        assertThat(result.keySet()).containsExactlyInAnyOrder(keyBytes0, keyBytes1, keyBytes2);
    }

    @Test
    public void putAndGetCompare()
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        mutatingKeyValueDb.put(keyBytes, valueBytes);
        final byte[] retVal = mutatingKeyValueDb.get(keyBytes);
        Assert.assertArrayEquals(valueBytes, retVal);
    }

    @Test
    public void testCas()
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{4, 5, 6, 7, 9, 9});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        mutatingKeyValueDb.put(keyBytes, valueBytes);

        final CasHolder<ByteArrayKey, byte[], Long> res = casKeyValueDb.getCas(keyBytes);
        Assert.assertEquals(keyBytes, res.getKey());
        Assert.assertEquals(Long.valueOf(0L), res.getCasValue());
        Assert.assertArrayEquals(valueBytes, res.getValue());

        final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsMakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
        res.setValue(valueBytesUpdated);
        casKeyValueDb.putCas(res);
    }

    @Test
    public void testIterate()
    {
        final Map<ByteArrayKey, byte[]> data = createFiveEntries();
        mutatingKeyValueDb.putAll(data);

        final IterableKeyValueDb<ByteArrayKey, byte[]> idb = (IterableKeyValueDb<ByteArrayKey, byte[]>) mutatingKeyValueDb;
        int count = 0;
        try (final SeekableIterator<ByteArrayKey, byte[]> iter = idb.iterator())
        {
            final ByteArrayKey prefixKey = new ByteArrayKey(new byte[]{1});
            assertThat(iter.seekTo(prefixKey)).isTrue();
            //iter.seekToFirst();
            while (iter.hasNext())
            {
                System.out.println(iter.next());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    private Map<ByteArrayKey, byte[]> createFiveEntries()
    {
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        final Map<ByteArrayKey, byte[]> data = new TreeMap<>();
        data.put(keyBytes0, valueBytes);
        data.put(keyBytes1, valueBytes);
        data.put(keyBytes2, valueBytes);
        data.put(keyBytes3, valueBytes);
        data.put(keyBytes4, valueBytes);
        return data;
    }

    @Test
    public void testMutate()
    {
        final ByteArrayKey key = new ByteArrayKey(new byte[]{6});
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsMakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsMakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);

        mutatingKeyValueDb.put(key, valueBytes);

        mutatingKeyValueDb.mutate(key, (Function<byte[], byte[]>) input -> valueBytesUpdated);

        final CasHolder<ByteArrayKey, byte[], Long> res = casKeyValueDb.getCas(key);
        Assert.assertEquals(Long.valueOf(1L), res.getCasValue());
        Assert.assertArrayEquals(valueBytesUpdated, res.getValue());
    }
}
