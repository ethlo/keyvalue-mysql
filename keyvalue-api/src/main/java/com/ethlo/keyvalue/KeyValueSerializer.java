package com.ethlo.keyvalue;

/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <NK>
 * @param <V>
 * @param <NV>
 */
public interface KeyValueSerializer<K, NK, V, NV>
{
	NK getKey(K key);
	
	K extractKey(NK nativeKey);
	
	NV getValue(V value);
	
	V extractValue(NV nativeValue);
}