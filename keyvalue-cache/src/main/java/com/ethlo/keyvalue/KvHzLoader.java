package com.ethlo.keyvalue;

/*-
 * #%L
 * Key/value cache
 * %%
 * Copyright (C) 2015 - 2018 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
