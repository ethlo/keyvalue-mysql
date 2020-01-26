package com.ethlo.keyvalue.range;

/*-
 * #%L
 * Key/value cache
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

import com.ethlo.keyvalue.range.GuavaRangeCache.EvictionPolicy;
import com.google.common.collect.Range;

public class GuavaRangeCacheTest
{
    @Test
    public void addRanges()
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(100);
        cache.put(Range.closed(0, 9), "value");
        cache.put(Range.closed(10, 19), "value2");
    }

    @Test
    public void getObjectInsideRange()
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(100);
        cache.put(Range.closed(0, 9), "value");
        cache.put(Range.closed(10, 19), "value2");
        assertThat(cache.get(11)).isEqualTo("value2");
    }

    @Test
    public void getObjectOutsideRange()
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(100);
        cache.put(Range.closed(0, 9), "value");
        cache.put(Range.closed(10, 19), "value2");
        assertThat(cache.get(100)).isNull();
    }

    @Test
    public void testTTLOnGet() throws InterruptedException
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(100, EvictionPolicy.LFU, 10, 100);
        cache.put(Range.closed(0, 9), "value");
        cache.put(Range.closed(10, 19), "value2");
        Thread.sleep(150);
        assertThat(cache.get(5)).isNull();
        assertThat(cache.get(15)).isNull();
    }

    @Test
    public void testTTLPerValue() throws InterruptedException
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(100, EvictionPolicy.LFU, 10, 0);
        cache.put(Range.closed(0, 9), "value", 100);
        cache.put(Range.closed(10, 19), "value2");
        Thread.sleep(150);
        assertThat(cache.get(5)).isNull();
        assertThat(cache.get(15)).isEqualTo("value2");
    }

    @Test
    public void testTTLOnGetEntry() throws InterruptedException
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(100, EvictionPolicy.LFU, 10, 100);
        cache.put(Range.closed(0, 9), "value");
        cache.put(Range.closed(10, 19), "value2");
        Thread.sleep(150);
        assertThat(cache.getEntry(5)).isNull();
        assertThat(cache.getEntry(15)).isNull();
    }

    @Test
    public void testEvictionLRU()
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(8);
        cache.put(Range.closed(0, 9), "value1");
        cache.put(Range.closed(10, 19), "value2");
        cache.put(Range.closed(20, 29), "value3");
        cache.put(Range.closed(30, 39), "value4");
        cache.put(Range.closed(40, 49), "value5");
        cache.put(Range.closed(50, 59), "value6");
        cache.put(Range.closed(60, 69), "value7");
        cache.put(Range.closed(70, 79), "value8");
        assertThat(cache.get(0)).isEqualTo("value1");
        assertThat(cache.get(10)).isEqualTo("value2");
        assertThat(cache.get(20)).isEqualTo("value3");
        assertThat(cache.get(30)).isEqualTo("value4");
        assertThat(cache.get(40)).isEqualTo("value5");
        assertThat(cache.get(50)).isEqualTo("value6");
        assertThat(cache.get(60)).isEqualTo("value7");
        assertThat(cache.get(70)).isEqualTo("value8");

        cache.put(Range.closed(80, 89), "value9");
        assertThat(cache.get(0)).isNull();
        assertThat(cache.get(10)).isNull();
        assertThat(cache.get(20)).isEqualTo("value3");
        assertThat(cache.get(30)).isEqualTo("value4");
        assertThat(cache.get(40)).isEqualTo("value5");
        assertThat(cache.get(50)).isEqualTo("value6");
        assertThat(cache.get(60)).isEqualTo("value7");
        assertThat(cache.get(70)).isEqualTo("value8");
        assertThat(cache.get(80)).isEqualTo("value9");
    }

    @Test
    public void testEvictionLFU()
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(4, EvictionPolicy.LFU, 25, 0);
        cache.put(Range.closed(0, 9), "value1");
        cache.put(Range.closed(10, 19), "value2");
        cache.put(Range.closed(20, 29), "value3");
        cache.put(Range.closed(30, 39), "value4");
        assertThat(cache.get(0)).isEqualTo("value1");
        assertThat(cache.get(10)).isEqualTo("value2");
        assertThat(cache.get(20)).isEqualTo("value3");
        assertThat(cache.get(30)).isEqualTo("value4");

        // Access count of first 3 items incremented
        cache.get(5);
        cache.get(15);
        cache.get(25);

        // Add one extra to trigger eviction
        cache.put(Range.closed(80, 89), "value9");

        assertThat(cache.get(0)).isEqualTo("value1");
        assertThat(cache.get(10)).isEqualTo("value2");
        assertThat(cache.get(20)).isEqualTo("value3");
        assertThat(cache.get(30)).isNull();
    }


    @Test(timeout = 5_000)
    public void testEvictionPerformance()
    {
        final RangeCache<Integer, String> cache = new GuavaRangeCache<>(10_000);
        for (int i = 0; i < 500_000; i++)
        {
            cache.put(Range.closed(i, i + 1), Integer.toString(i));
        }
    }
}
