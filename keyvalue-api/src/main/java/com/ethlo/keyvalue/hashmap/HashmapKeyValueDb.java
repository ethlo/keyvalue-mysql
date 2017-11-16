package com.ethlo.keyvalue.hashmap;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

import com.ethlo.keyvalue.CasHolder;
import com.ethlo.keyvalue.CasKeyValueDb;
import com.ethlo.keyvalue.HexKeyEncoder;
import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.SeekableIterator;
import com.ethlo.keyvalue.keys.ByteArrayKey;

/**
 * 
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
            public void seekToFirst()
            {
                iter = data.entrySet().iterator();
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
            public void seekTo(ByteArrayKey key)
            {
                final String targetKey = enc.toString(key.getByteArray());
                seekToFirst();
                while (iter.hasNext())
                {
                    final Entry<ByteArrayKey, byte[]> e = iter.next();
                    final String hexKey = enc.toString(e.getKey().getByteArray());
                    if (hexKey.startsWith(targetKey))
                    {
                        return;
                    }
                }
                throw new NoSuchElementException(targetKey);
            }

            @Override
            public void close()
            {
                
            }

            @Override
            public void seekToLast()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}