package com.ethlo.keyvalue;

import java.util.LinkedList;
import java.util.List;

import com.ethlo.keyvalue.keys.Key;

/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 * @param <C>
 */
public class BatchCasWriteWrapper<K extends Key, V, C> implements BatchCasKeyValueDb<K, V, C>
{
	private final List<CasHolder<K, V, C>> buffer = new LinkedList<>();
	private final BatchCasKeyValueDb<K,V,C> kvdb;
	private int batchSize;;
		
	public BatchCasWriteWrapper(BatchCasKeyValueDb<K,V,C> kvdb, int batchSize)
	{
		this.kvdb = kvdb;
		this.batchSize = batchSize;
	}

	public void putCas(CasHolder<K, V, C> casHolder)
	{
		this.buffer.add(casHolder);
		if (buffer.size() >= batchSize)
		{
			flush();
		}
	}

	public V get(K key)
	{
		return kvdb.get(key);
	}

	public void putBatch(List<CasHolder<K, V, C>> casList)
	{
		kvdb.putBatch(casList);
	}

	public void put(K key, V value)
	{
		kvdb.put(key, value);
	}

	public void delete(K key)
	{
		kvdb.delete(key);
	}

	public void clear()
	{
		kvdb.clear();
	}

	public void close()
	{
		kvdb.close();
	}

	public CasHolder<K, V, C> getCas(K key)
	{
		return kvdb.getCas(key);
	}

	public void flush()
	{
		this.kvdb.putBatch(buffer);
		buffer.clear();
	}
}
