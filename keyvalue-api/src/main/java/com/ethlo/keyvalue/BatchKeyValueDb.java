package com.ethlo.keyvalue;

import com.ethlo.keyvalue.keys.Key;


/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 */
public interface BatchKeyValueDb<K extends Key,V> extends KeyValueDb<K, V>
{
	void flush(BatchWriteWrapper<K, V> batch);
}
