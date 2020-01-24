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

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.springframework.util.Assert;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

/**
 * @param <K> Key type
 * @param <V> Value type
 * @author Morten Haraldsen
 */
@SuppressWarnings("UnstableApiUsage")
public class GuavaRangeCache<K extends Comparable<K>, V> implements RangeCache<K, V>
{
    private final RangeMap<K, AccessData<V>> backingMap = TreeRangeMap.create();
    private final int maxSize;
    private final EvictionPolicy evictionPolicy;
    private final int evictionPercent;
    private final long ttl;

    public enum EvictionPolicy
    {
        LRU, LFU
    }

    public GuavaRangeCache()
    {
        this(Integer.MAX_VALUE);
    }

    public GuavaRangeCache(int maxSize)
    {
        this(maxSize, EvictionPolicy.LRU);
    }

    public GuavaRangeCache(int maxSize, EvictionPolicy evictionPolicy)
    {
        this(maxSize, evictionPolicy, 25, 0);
    }

    public GuavaRangeCache(int maxSize, EvictionPolicy evictionPolicy, int evictionPercent, long ttlMs)
    {
        Assert.isTrue(maxSize > 0, "Max size must be greater than 0");
        this.maxSize = maxSize;
        Assert.notNull(evictionPolicy, "Eviction policy cannot be null");
        this.evictionPolicy = evictionPolicy;
        Assert.isTrue(evictionPercent > 0 && evictionPercent <= 100, "Eviction percent must be between 1 and 100");
        this.evictionPercent = evictionPercent;
        this.ttl = ttlMs;
    }

    @Override
    public V get(K key)
    {
        synchronized (backingMap)
        {
            final Entry<Range<K>, AccessData<V>> entry = backingMap.getEntry(key);
            if (entry != null)
            {
                if (expired(entry.getValue()))
                {
                    remove(entry.getKey());
                    return null;
                }
                entry.getValue().accessed();
                return entry.getValue().getData();
            }
            return null;
        }
    }

    private boolean expired(AccessData<V> access)
    {
        final long ttl = access.getTTL() != null ? access.getTTL() : this.ttl;
        if (ttl > 0)
        {
            return System.currentTimeMillis() > (access.getLastAccessTime() + ttl);
        }
        return false;
    }

    @Override
    public Entry<Range<K>, V> getEntry(K key)
    {
        synchronized (backingMap)
        {
            final Entry<Range<K>, AccessData<V>> retVal = backingMap.getEntry(key);
            if (retVal != null)
            {
                if (ttl > 0)
                {
                    if (expired(retVal.getValue()))
                    {
                        remove(retVal.getKey());
                        return null;
                    }
                }
                retVal.getValue().accessed();
                return new AbstractMap.SimpleEntry<>(retVal.getKey(), retVal.getValue().getData());
            }
            return null;
        }
    }

    @Override
    public Range<K> span()
    {
        synchronized (backingMap)
        {
            return backingMap.span();
        }
    }

    @Override
    public void put(Range<K> range, V value)
    {
        synchronized (backingMap)
        {
            evictIfNecessary();
            backingMap.put(range, new AccessData<V>(range).setData(value));
        }
    }

    @Override
    public void put(Range<K> range, V value, long ttl)
    {
        synchronized (backingMap)
        {
            evictIfNecessary();
            backingMap.put(range, new AccessData<V>(range).setData(value).setTTL(ttl));
        }
    }

    private void evictIfNecessary()
    {
        final int curCount = getCount(backingMap);
        if (curCount >= maxSize)
        {
            final List<AccessData<V>> accesses = new ArrayList<>(backingMap.asMapOfRanges().values());

            // Sort
            accesses.sort(this.evictionPolicy == EvictionPolicy.LRU ? LRU_COMP : LFU_COMP);

            // Actual remove
            int evicted = 0;
            final int toEvict = (int) (maxSize * ((double) (evictionPercent) / 100));
            final Iterator<AccessData<V>> accessDataIterator = accesses.iterator();
            while (evicted < toEvict && accessDataIterator.hasNext())
            {
                final AccessData<V> access = accessDataIterator.next();
                backingMap.remove(access.getRange());
                evicted++;
            }
        }
    }

    @Override
    public void clear()
    {
        synchronized (backingMap)
        {
            this.backingMap.clear();
        }
    }

    @Override
    public void remove(Range<K> range)
    {
        synchronized (backingMap)
        {
            this.backingMap.remove(range);
        }
    }

    private int getCount(RangeMap<K, AccessData<V>> rangeMap)
    {
        return rangeMap.asMapOfRanges().size();
    }

    private class AccessData<D> implements Serializable
    {
        private static final long serialVersionUID = 5619216026006141788L;

        private final Range<K> range;
        private D data;
        private long lastAccessTime;
        private long accessCount;
        private Long ttl;

        public AccessData(Range<K> range)
        {
            this.range = range;
            accessed();
        }

        public Long getTTL()
        {
            return this.ttl;
        }

        public AccessData<D> setTTL(Long ttl)
        {
            this.ttl = ttl;
            return this;
        }

        public AccessData<D> setData(D data)
        {
            this.data = data;
            return this;
        }

        public D getData()
        {
            return data;
        }

        public void accessed()
        {
            accessCount++;
            lastAccessTime = System.currentTimeMillis();
        }

        public Range<K> getRange()
        {
            return range;
        }

        public long getLastAccessTime()
        {
            return lastAccessTime;
        }

        public long getAccessCount()
        {
            return accessCount;
        }
    }

    private final Comparator<AccessData<V>> LFU_COMP = Comparator.comparingLong(AccessData::getAccessCount);

    private final Comparator<AccessData<V>> LRU_COMP = Comparator.comparingLong(AccessData::getLastAccessTime);

    @Override
    public long getRangeCount()
    {
        return this.getCount(backingMap);
    }
}
