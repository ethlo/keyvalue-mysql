package com.ethlo.keyvalue;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ethlo.keyvalue.keys.ByteArrayKey;

/**
 * 
 * @author Morten Haraldsen
 *
 */
public class KeyValueDbTest extends AbstractKeyValueDbTest
{
	@Resource(name="kvDbManager")
	private KeyValueDbManager<ByteArrayKey, byte[], KeyValueDb<ByteArrayKey, byte[]>> dbManager;
	
	private KeyValueDb<ByteArrayKey, byte[]> db;
	
	@Before
	public void fetchDb()
	{
		this.db = this.dbManager.getDb("test", true, new HexKeyEncoder(), new NopDataCompressor());
	}
	
	@Test
	public void testPut()
	{
		final ByteArrayKey key = new ByteArrayKey("abcd".getBytes());
		final byte[] value = "fghijklmnopqrstuvwxyz".getBytes();
		db.put(key, value);
		final byte[] retVal = db.get(key);
		Assert.assertArrayEquals(value, retVal);
	}
	
	@Test
	public void testGet()
	{
		final ByteArrayKey key = new ByteArrayKey("abcd".getBytes());
		final byte[] value = "fghijklmnopqrstuvwxyz".getBytes();
		db.put(key, value);
		final byte[] retVal = db.get(key);
		Assert.assertArrayEquals(value, retVal);
	}
	
	@Test
	public void testDelete()
	{
		final ByteArrayKey key = new ByteArrayKey("abcd".getBytes());
		final byte[] value = "fghijklmnopqrstuvwxyz".getBytes();
		db.put(key, value);
		final byte[] retVal = db.get(key);
		Assert.assertArrayEquals(value, retVal);
		db.delete(key);
		Assert.assertNull(db.get(key));
	}
	
	@Test
	public void testClear()
	{
		final ByteArrayKey key = new ByteArrayKey("abcd".getBytes());
		final ByteArrayKey key2 = new ByteArrayKey("ef".getBytes());
		final byte[] value = "fghijklmnopqrstuvwxyz".getBytes();
		db.put(key, value);
		db.put(key2, value);
		final byte[] retVal = db.get(key);
		final byte[] retVal2 = db.get(key2);
		Assert.assertArrayEquals(value, retVal);
		Assert.assertArrayEquals(value, retVal2);
		db.clear();
		Assert.assertNull(db.get(key));
		Assert.assertNull(db.get(key2));
	}
}
