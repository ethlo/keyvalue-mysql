package com.ethlo.keyvalue;

import com.ethlo.keyvalue.keys.Key;

/**
 * 
 * @author mha
 */
public interface IterableKeyValueDb<K extends Key,V> extends KeyValueDb<K,V>
{
	SeekableIterator<K,V> iterator();
}
