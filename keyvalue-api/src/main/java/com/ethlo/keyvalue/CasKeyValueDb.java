package com.ethlo.keyvalue;

import com.ethlo.keyvalue.keys.Key;

/**
 * Extension of {@link KeyValueDb} that allows optimistic locking using CAS (compare-and-swap/check-and-set).
 * 
 * This can be used to prevent clients from updating values in the database that may have changed since the 
 * client obtained the value. Methods for storing and updating information support a CAS method that allows 
 * you to ensure that the client is updating the version of the data that the client retrieved.
 * 
 * @author Morten Haraldsen
 * @param <K> Key type
 * @param <V> Value type
 * @param <C> CAS type
 */
public interface CasKeyValueDb<K extends Key,V,C> extends KeyValueDb<K, V>
{
	CasHolder<K,V,C> getCas(K key);
	
	void putCas(CasHolder<K,V,C> cas);
}