package com.ethlo.keyvalue;

import com.ethlo.keyvalue.keys.Key;

/**
 * The minimal operations needed to be supported
 * 
 *  @see CasKeyValueDb
 *  @see BatchCasKeyValueDb
 *  
 * @author Morten Haraldsen
 */
public interface KeyValueDb<K extends Key, V> extends AutoCloseable
{
	V get(K key);

	void put(K key, V value);
	
	void delete(K key);

	void clear();
	
	void close();
}