package com.ethlo.mycached;

import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.commons.codec.binary.Base64;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.CasHolder;
import com.ethlo.keyvalue.CasKeyValueDb;

/**
 * 
 * @author mha
 */
public class MyCachedClientImpl implements CasKeyValueDb<byte[],byte[], Long>
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
	public byte[] get(byte[] key)
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

	private String getKey(byte[] key)
	{
		return "@@" + this.schemaName + "_" + this.tableName + "." + Base64.encodeBase64String(key);
	}

	@Override
	public void put(byte[] key, byte[] value)
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
	public void delete(byte[] key)
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
	public CasHolder<byte[], byte[], Long> getCas(byte[] key)
	{
		// TODO:
		throw new UnsupportedOperationException();
	}

	@Override
	public void putCas(CasHolder<byte[], byte[], Long> cas)
	{
		// TODO:
		throw new UnsupportedOperationException();
	}
}