package com.ethlo.mycached;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;

import com.ethlo.keyvalue.CasHolder;
import com.ethlo.keyvalue.CasKeyValueDb;
import com.ethlo.keyvalue.MutatingKeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.google.common.base.Function;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/mycached-testcontext.xml"})
@TestExecutionListeners(listeners={DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class})
public class SmokeTest
{
	@Resource
	private LegacyMyCachedClientManagerImpl clientManager;
	
	private CasKeyValueDb<ByteArrayKey, byte[], Long> client;
	
	@Before
	public void setup()
	{
		final String dbName = "someTestData";
		this.client = clientManager.createMainDb(dbName, true);
	}
	
	@Test
	public void putAndGetCompare() throws SQLException
	{
		final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{0,1,2,3,4,5,6,7});
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		client.put(keyBytes, valueBytes);
		final byte[] retVal = client.get(keyBytes);
		Assert.assertArrayEquals(valueBytes, retVal);
	}
	
	@Test
	public void testCas() throws SQLException
	{
		final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{4,5,6,7,9,9});
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsmakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
		
		client.put(keyBytes, valueBytes);
		
		final CasHolder<ByteArrayKey, byte[], Long> res = client.getCas(keyBytes);
		Assert.assertEquals(Long.valueOf(0L), res.getCasValue());
		Assert.assertArrayEquals(valueBytes, res.getValue());
		
		res.setValue(valueBytesUpdated);
		client.putCas(res);
	}
	
	@Test
	public void testMutate() throws SQLException
	{
		final ByteArrayKey key = new ByteArrayKey(new byte[]{4,5,6,7,9,9});
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsmakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
		
		client.put(key, valueBytes);
		
		@SuppressWarnings("unchecked")
		final MutatingKeyValueDb<ByteArrayKey, byte[]> mdb = (MutatingKeyValueDb<ByteArrayKey, byte[]>) client;
		mdb.mutate(key, new Function<byte[], byte[]>()
		{
			@Override
			public byte[] apply(byte[] input)
			{
				return valueBytesUpdated;
			}
		});
		
		final CasHolder<ByteArrayKey, byte[], Long> res = client.getCas(key);
		Assert.assertEquals(Long.valueOf(1L), res.getCasValue());
		Assert.assertArrayEquals(valueBytesUpdated, res.getValue());
	}
}
