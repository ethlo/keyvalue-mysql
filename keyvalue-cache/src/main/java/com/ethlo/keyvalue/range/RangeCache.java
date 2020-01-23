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

import java.util.Map.Entry;

import com.google.common.collect.Range;

/**
 * A cache of disjoint nonempty ranges to non-null values. Queries look up the value
 * associated with the range (if any) that contains a specified key.
 * 
 * @param <K> Key type
 * @param <V> Value type
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
