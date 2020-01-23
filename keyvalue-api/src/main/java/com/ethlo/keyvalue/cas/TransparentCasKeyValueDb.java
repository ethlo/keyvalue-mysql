package com.ethlo.keyvalue.cas;

/*-
 * #%L
 * Key/Value API
 * %%
 * Copyright (C) 2013 - 2020 Morten Haraldsen (ethlo)
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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ethlo.keyvalue.keys.Key;


/**
 * Throw-away wrapper around a single batch of writes for key-value database, allowing the CAS values to be transparently handled.
 *
 * @param <K>
 * @param <V>
 * @param <C>
 * @author Morten Haraldsen
 */
public class TransparentCasKeyValueDb<K extends Key, V, C extends Comparable<C>> implements CasKeyValueDb<K, V, C>
{
    private final CasKeyValueDb<K, V, C> db;
    private final ConcurrentHashMap<K, Optional<C>> revisionHolder;
    private static final Logger logger = LoggerFactory.getLogger(TransparentCasKeyValueDb.class);

    public TransparentCasKeyValueDb(CasKeyValueDb<K, V, C> casKeyValueDb)
    {
        this.db = casKeyValueDb;
        this.revisionHolder = new ConcurrentHashMap<>(16, 0.75f, 4);
    }

    @Override
    public V get(K key)
    {
        final CasHolder<K, V, C> casHolder = this.db.getCas(key);
        if (casHolder != null)
        {
            final C currentCasValue = casHolder.getCasValue();
            //
            //			// Optimization, fail early if data has been modified
            //			final Optional<C> prevCasValue = this.revisionHolder.get(casHolder.getKey());
            //			if (prevCasValue != null && prevCasValue.isPresent() && !currentCasValue.equals(prevCasValue.get()))
            //			{
            //				throw new ConcurrentModificationException("Value for key " + key + " has been concurrently modified");
            //			}
            //
            this.revisionHolder.put(key, Optional.of(currentCasValue));
            logger.debug("get({}) with CAS value {}", key, currentCasValue);
            return casHolder.getValue();
        }
        this.revisionHolder.put(key, Optional.empty());
        logger.debug("get({}) with empty result", key);
        return null;
    }

    @Override
    public void put(K key, V value)
    {
        final Optional<C> casValue = this.revisionHolder.get(key);
        if (casValue != null)
        {
            final C cas = casValue.isPresent() ? casValue.get() : null;
            logger.debug("put({}) with CAS value {}", key, cas);
            try
            {
                this.db.putCas(new CasHolder<>(cas, key, value));
            } finally
            {
                this.revisionHolder.remove(key);
            }
        }
    }

    @Override
    public void delete(K key)
    {
        this.db.delete(key);
    }

    @Override
    public void clear()
    {
        this.db.clear();
    }

    @Override
    public void close()
    {
        this.db.close();
    }

    @Override
    public CasHolder<K, V, C> getCas(K key)
    {
        return db.getCas(key);
    }

    @Override
    public void putCas(CasHolder<K, V, C> cas)
    {
        db.putCas(cas);
    }
}
