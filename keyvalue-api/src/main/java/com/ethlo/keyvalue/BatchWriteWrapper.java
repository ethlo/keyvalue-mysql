package com.ethlo.keyvalue;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 */
public class BatchWriteWrapper<K, V>
{
	private final Map<K, V> buffer = new LinkedHashMap<>();
		
	public void put(K key, V value)
	{
		this.buffer.put(key, value);
	}

	public Map<K, V> data()
	{
		return this.buffer;
	}
}