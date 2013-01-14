package com.ethlo.mycached;


import java.io.IOException;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.commons.codec.binary.Base64;
import org.springframework.util.Assert;

/**
 * 
 * @author mha
 */
public class MyCachedClientImpl implements MyCachedClient
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
		catch (TimeoutException | InterruptedException | MemcachedException | IOException e)
		{
			throw new MyCachedIoException(e.getMessage(), e);
		}
	}

	private String getKey(byte[] key)
	{
		//return "@@" + this.schemaName + "_" + this.tableName + "." + Base64.encodeBase64String(key);
		return this.tableName + "." + Base64.encodeBase64String(key);
	}

	@Override
	public boolean set(byte[] key, byte[] value)
	{
		try
		{
			return this.client.set(getKey(key), 0, CompressionUtil.compress(value));
		}
		catch (TimeoutException | InterruptedException | MemcachedException | IOException e)
		{
			throw new MyCachedIoException(e.getMessage(), e);
		}
	}

	@Override
	public boolean delete(byte[] key)
	{
		try
		{
			return this.client.delete(getKey(key));
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
}