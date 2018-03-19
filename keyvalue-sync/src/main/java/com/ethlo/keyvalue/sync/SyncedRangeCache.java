package com.ethlo.keyvalue.sync;

import java.util.Map.Entry;

import com.ethlo.keyvalue.range.RangeCache;
import com.google.common.collect.Range;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;

/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 */
public class SyncedRangeCache<K extends Comparable<K>,V> implements RangeCache<K,V>
{
	private final RangeCache<K, V> delegate;
	private ITopic<RangeCacheInvalidationMessage<K>> topic;
    private String memberUuid;

	public SyncedRangeCache(RangeCache<K, V> rangeCache, HazelcastInstance hazelcastInstance, final String cacheId)
	{
		this.topic = hazelcastInstance.getTopic("_invalidation_" + cacheId);
		this.memberUuid = hazelcastInstance.getCluster().getLocalMember().getUuid();
		this.delegate = rangeCache;
		topic.addMessageListener(message->
		{
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