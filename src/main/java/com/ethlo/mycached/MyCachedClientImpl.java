package com.ethlo.mycached;


import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.commons.codec.binary.Base64;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.BatchCasKeyValueDb;
import com.ethlo.keyvalue.BatchCasWriteWrapper;
import com.ethlo.keyvalue.CasHolder;

/**
 * 
 * @author mha
 */
public class MyCachedClientImpl implements BatchCasKeyValueDb<ByteBuffer,byte[], Long>
{
	private MemcachedClient client;
	private String schemaName;
	private String tableName;
	
	public MyCachedClientImpl(String schemaName, String tableName, MemcachedClient memcachedClient)
	{
		Assert.hasLength(schemaName, "schemaName cannot be null");
		Assert.hasLength(tableName, "tableName cannot be null");
		Assert.notNull(memcachedClient, "memcachedClient cannot be null");
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.client = memcachedClient;
	}
	
	@Override
	public byte[] get(ByteBuffer key)
	{
		try
		{
			final String mmKey = getKey(key);
			final byte[] raw = this.client.get(mmKey);
			return CompressionUtil.uncompress(raw);
		}
		catch (TimeoutException | InterruptedException | MemcachedException e)
		{
			throw new MyCachedIoException(e.getMessage(), e);
		}
	}

	private String getKey(ByteBuffer key)
	{
		return "@@" + this.schemaName + "_" + this.tableName + "." + Base64.encodeBase64String(key.array());
	}

	@Override
	public void put(ByteBuffer key, byte[] value)
	{
		try
		{
			this.client.set(getKey(key), 0, CompressionUtil.compress(value));
		}
		catch (TimeoutException | InterruptedException | MemcachedException e)
		{
			throw new MyCachedIoException(e.getMessage(), e);
		}
	}

	@Override
	public void delete(ByteBuffer key)
	{
		try
		{
			this.client.delete(getKey(key));
		}
		catch (TimeoutException | InterruptedException | MemcachedException e)
		{
			throw new MyCachedIoException(e.getMessage(), e);
		}
	}

	@Override
	public void clear()
	{

	}

	@Override
	public void close()
	{
		
	}

	@Override
	public CasHolder<ByteBuffer, byte[], Long> getCas(ByteBuffer key)
	{
		// TODO:
		throw new UnsupportedOperationException();
	}

	@Override
	public void putCas(CasHolder<ByteBuffer, byte[], Long> cas)
	{
		// TODO:
		throw new UnsupportedOperationException();
	}

	@Override
	public void flush(BatchCasWriteWrapper<ByteBuffer, byte[], Long> wrapper)
	{
		// TODO:
		throw new UnsupportedOperationException();
	}
}