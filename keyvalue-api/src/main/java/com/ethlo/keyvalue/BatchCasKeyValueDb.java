package com.ethlo.keyvalue;

import java.util.List;

import com.ethlo.keyvalue.keys.Key;


/**
 * Extension of {@link CasKeyValueDb} that allows to do batched writes.
 * 
 * @author Morten Haraldsen
 * 
 */
public interface BatchCasKeyValueDb<K extends Key,V,C> extends CasKeyValueDb<K,V,C>
{
	void putBatch(List<CasHolder<K,V,C>> casList);
}