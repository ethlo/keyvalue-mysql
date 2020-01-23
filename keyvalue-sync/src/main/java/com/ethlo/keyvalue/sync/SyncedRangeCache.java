package com.ethlo.keyvalue.sync;

/*-
 * #%L
 * Key/Value sync
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

import java.util.Map.Entry;

import com.ethlo.keyvalue.range.RangeCache;
import com.google.common.collect.Range;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;

/**
 * @param <K> Key type
 * @param <V> Value type
 */
public class SyncedRangeCache<K extends Comparable<K>, V> implements RangeCache<K, V>
{
    private final RangeCache<K, V> delegate;
    private ITopic<RangeCacheInvalidationMessage<K>> topic;
    private String memberUuid;

    public SyncedRangeCache(RangeCache<K, V> rangeCache, HazelcastInstance hazelcastInstance, final String cacheId)
    {
        this.topic = hazelcastInstance.getTopic("_invalidation_" + cacheId);
        this.memberUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();
        this.delegate = rangeCache;
        topic.addMessageListener(message -> {
            if (message.getMessageObject().getMemberUuid().equals(memberUuid))
            {
                // Skip local message
                return;
            }

            final RangeCacheInvalidationMessage<K> val = message.getMessageObject();
            if (val.isAll())
            {
                delegate.clear();
            }
            else
            {
                for (Range<K> range : val.getKeys())
                {
                    delegate.remove(range);
                }
            }
        });

    }

    @Override
    public V get(K key)
    {
        return delegate.get(key);
    }

    @Override
    public Entry<Range<K>, V> getEntry(K key)
    {
        return delegate.getEntry(key);
    }

    @Override
    public Range<K> span()
    {
        return delegate.span();
    }

    @Override
    public void put(Range<K> range, V value)
    {
        this.put(range, value, 0);
    }

    @Override
    public void clear()
    {
        delegate.clear();
        topic.publish(new RangeCacheInvalidationMessage<K>(memberUuid));
    }

    @Override
    public void remove(Range<K> range)
    {
        delegate.remove(range);
        topic.publish(new RangeCacheInvalidationMessage<K>(memberUuid, range));
    }

    @Override
    public long getRangeCount()
    {
        return delegate.getRangeCount();
    }

    @Override
    public void put(Range<K> range, V value, long ttl)
    {
        delegate.put(range, value, ttl);
        topic.publish(new RangeCacheInvalidationMessage<K>(memberUuid, range));
    }
}
