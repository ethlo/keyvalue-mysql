package com.ethlo.keyvalue;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.springframework.dao.OptimisticLockingFailureException;

import com.ethlo.keyvalue.keys.Key;

public class KvHzLoader<K extends Key,V,C> implements HzLoadStore<K, CasHolder<K,V,C>>
{
	private BatchCasKeyValueDb<K, V, C> delegate;

	public KvHzLoader(BatchCasKeyValueDb<K, V, C> delegate)
	{
		this.delegate = delegate;
	}
	
	@Override
	public CasHolder<K,V,C> load(K key)
	{
		return delegate.getCas(key);
	}

	@Override
	public Map<K,CasHolder<K,V,C>> loadAll(Collection<K> keys)
	{
		return null;
	}

	@Override
	public Set<K> loadAllKeys()
	{
		return null;
	}

	@Override
	public void store(K key, CasHolder<K,V,C> value)
	{
		if (value.getCasValue() != null)
		{
			try
			{
				delegate.putCas(value);
			}
			catch(OptimisticLockingFailureException exc)
			{
				final CasHolder<K, V, C> delegateValue = delegate.getCas(key);
				if (delegateValue != null)
				{
					delegateValue.setValue(value.getValue());
					store(key, delegateValue);
				}
				else
				{
					// Value was deleted in the meantime
					store(key, new CasHolder<K, V, C>(null, key, value.getValue()));
				}
			}
		}
		else
		{
			delegate.put(key, value.getValue());
		}
	}

	@Override
	public void storeAll(Map<K, CasHolder<K,V,C>> map)
	{
		for (Entry<K, CasHolder<K, V, C>> e : map.entrySet())
		{
			store(e.getKey(), e.getValue());
		}
	}

	@Override
	public void delete(K key)
	{
		delegate.delete(key);
	}

	@Override
	public void deleteAll(Collection<K> keys)
	{
		for (K key : keys)
		{
			this.delete(key);
		}
	}
}
