package com.ethlo.keyvalue.hashmap;

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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.SeekableIterator;
import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;

/**
 * @author mha
 */
public class HashmapKeyValueDb implements CasKeyValueDb<ByteArrayKey, byte[], Long>, IterableKeyValueDb<ByteArrayKey, byte[]>
{
    private ConcurrentSkipListMap<ByteArrayKey, byte[]> data = new ConcurrentSkipListMap<>();

    @Override
    public byte[] get(ByteArrayKey key)
    {
        return data.get(key);
    }

    @Override
    public void put(ByteArrayKey key, byte[] value)
    {
        this.data.put(key, value);
    }

    @Override
    public void clear()
    {
        this.data.clear();
    }

    @Override
    public void close()
    {

    }

    @Override
    public CasHolder<ByteArrayKey, byte[], Long> getCas(ByteArrayKey key)
    {
        //FIXME: Faking it!
        final byte[] value = this.get(key);
        if (value != null)
        {
            return new CasHolder<ByteArrayKey, byte[], Long>(0L, key, value);
        }
        return null;
    }

    @Override
    public void putCas(CasHolder<ByteArrayKey, byte[], Long> casHolder)
    {
        //FIXME: Faking it!
        this.put(casHolder.getKey(), casHolder.getValue());
    }

    @Override
    public void delete(ByteArrayKey key)
    {
        this.data.remove(key);
    }

    public long getTotalSize()
    {
        long total = 0;
        for (Entry<ByteArrayKey, byte[]> data : this.data.entrySet())
        {
            total += data.getValue().length;
        }
        return total;
    }

    @Override
    public SeekableIterator<ByteArrayKey, byte[]> iterator()
    {
        return new SeekableIterator<ByteArrayKey, byte[]>()
        {
            private final HexKeyEncoder enc = new HexKeyEncoder();
            private Iterator<Entry<ByteArrayKey, byte[]>> iter;

            @Override
            public boolean seekToFirst()
            {
                iter = data.entrySet().iterator();
                return !data.isEmpty();
            }

            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            @Override
            public Entry<ByteArrayKey, byte[]> next()
            {
                return iter.next();
            }

            @Override
            public boolean hasPrevious()
            {
                return false;
            }

            @Override
            public Entry<ByteArrayKey, byte[]> previous()
            {
                throw new NoSuchElementException();
            }

            @Override
            public boolean seekTo(ByteArrayKey key)
            {
                final String targetKey = enc.toString(key.getByteArray());
                seekToFirst();
                while (iter.hasNext())
                {
                    final Entry<ByteArrayKey, byte[]> e = iter.next();
                    final String hexKey = enc.toString(e.getKey().getByteArray());
                    if (hexKey.startsWith(targetKey))
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void close()
            {

            }

            @Override
            public boolean seekToLast()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
