package com.ethlo.keyvalue;

import com.ethlo.keyvalue.keys.Key;
import com.google.common.base.Function;

/**
 * Extension of {@link KeyValueDb} that allows atomic mutation of a single value.
 * 
 * @author Morten Haraldsen
 * @param <K> Key type
 * @param <V> Value type
 */
public interface MutatingKeyValueDb<K extends Key,V> extends KeyValueDb<K, V>
{
	/**
	 * Value mutator method that should handle atomic updates using optimistic/pessimistic locking
	 * @param key
	 * @param mutator
	 */
	void mutate(K key, Function<V, V> mutator);
}