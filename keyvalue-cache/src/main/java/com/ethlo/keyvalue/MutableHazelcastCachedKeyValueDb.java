package com.ethlo.keyvalue;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.dao.OptimisticLockingFailureException;

import com.ethlo.keyvalue.keys.Key;
import com.google.common.base.Function;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Key/value distributed cache with configurable write/through or write-behind for speeding up data read/writes
 * 
 * @author Morten Haraldsen
 */
public class MutableHazelcastCachedKeyValueDb<K extends Key, V, C> implements MutatingKeyValueDb<K, V>, BatchCasKeyValueDb<K, V, C>
{
	private final BatchCasKeyValueDb<K, V, C> delegate;
	private IMap<K, CasHolder<K,V,C>> cacheMap;
	
	public MutableHazelcastCachedKeyValueDb(BatchCasKeyValueDb<K, V, C> delegate, HazelcastInstance hazelcastInstance, MapConfig mapConfig, String mapName)
	{
		this.delegate = delegate;
		this.cacheMap = setupMap(hazelcastInstance, mapName, mapConfig);
	}
	
	private IMap<K, CasHolder<K,V,C>> setupMap(HazelcastInstance hazelcastInstance, String mapName, MapConfig mapConfig)
	{
		// Set map store
		mapConfig.getMapStoreConfig().setImplementation(new KvHzLoader<K,V,C>(delegate));
		hazelcastInstance.getConfig().getMapConfigs().put(mapName, mapConfig);
		return hazelcastInstance.getMap(mapName);
	}

	@Override
	public void clear()
	{
		this.delegate.clear();
		this.cacheMap.clear();
	}

	@Override
	public void close()
	{
		delegate.close();
		this.cacheMap.destroy();
	}

	@Override
	public void delete(K key)
	{
		this.cacheMap.delete(key);
	}

	@Override
	public V get(K key)
	{
		final CasHolder<K, V, C> wrapper = this.cacheMap.get(key);
		return wrapper != null ? wrapper.getValue() : null;
	}

	@Override
	public void put(K key, V value)
	{
		this.cacheMap.set(key, new CasHolder<K,V,C>(null, key, value));
	}

	@Override
	public void mutate(K key, Function<V, V> mutator)
	{
		final CasHolder<K, V, C> existing = this.getCas(key);
		final V output = mutator.apply(existing != null ? existing.getValue() : null);
		if (existing != null)
		{
			// Replace existing
			final boolean replaced = this.cacheMap.replace(key, existing, new CasHolder<K,V,C>(existing.getCasValue(), key, output));
			if (! replaced)
			{
				throw new OptimisticLockingFailureException("Could not mutate data for key " + key);
			}
		}
		else
		{
			// No existing version
			if (this.cacheMap.put(key, new CasHolder<K,V,C>(null, key, output)) != null)
			{
				throw new OptimisticLockingFailureException("Could not mutate data for key " + key);
			}
		}
	}

	@Override
	public CasHolder<K, V, C> getCas(K key)
	{
		return this.cacheMap.get(key);
	}

	@Override
	public void putCas(CasHolder<K, V, C> cas)
	{
		this.cacheMap.put(cas.getKey(), cas);
	}

	@Override
	public void putBatch(List<CasHolder<K, V, C>> casList)
	{
		final Map<K,CasHolder<K,V,C>> insert = new TreeMap<K, CasHolder<K,V,C>>();
		for (CasHolder<K, V, C> e : casList)
		{
			insert.put(e.getKey(), e);
		}
		this.cacheMap.putAll(insert);
	}
}
