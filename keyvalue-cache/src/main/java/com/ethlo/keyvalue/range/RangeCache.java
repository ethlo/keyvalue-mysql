package com.ethlo.keyvalue.range;

import java.util.Map.Entry;

import com.google.common.collect.Range;

/**
 * A cache of disjoint nonempty ranges to non-null values. Queries look up the value
 * associated with the range (if any) that contains a specified key.
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 */
public interface RangeCache<K extends Comparable<K>, V>
{
	V get(K key);

	Entry<Range<K>, V> getEntry(K key);

	Range<K> span();

	void put(Range<K> range, V value);

	void clear();

	void remove(Range<K> range);

	long getRangeCount();

	void put(Range<K> range, V value, long ttl);
}