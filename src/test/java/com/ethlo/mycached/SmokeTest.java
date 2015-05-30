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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/mycached-testcontext.xml"})
@TestExecutionListeners(listeners={DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class})
public class SmokeTest
{
	@Resource
	private LegacyMyCachedClientManagerImpl clientManager;
	
	private CasKeyValueDb<byte[], byte[], Long> client;
	
	@Before
	public void setup()
	{
		final String dbName = "someTestData";
		this.client = clientManager.createMainDb(dbName, true);
	}
	
	@Test
	public void putAndGetCompare() throws SQLException
	{
		final byte[] keyBytes = new byte[]{0,1,2,3,4,5,6,7};
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		client.put(keyBytes, valueBytes);
		final byte[] retVal = client.get(keyBytes);
		Assert.assertArrayEquals(valueBytes, retVal);
	}
	
	@Test
	public void testCas() throws SQLException
	{
		final byte[] keyBytes = new byte[]{4,5,6,7,9,9};
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsmakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
		
		client.put(keyBytes, valueBytes);
		
		final CasHolder<byte[], byte[], Long> res = client.getCas(keyBytes);
		Assert.assertEquals(Long.valueOf(0L), res.getCasValue());
		Assert.assertArrayEquals(valueBytes, res.getValue());
		
		res.setValue(valueBytesUpdated);
		client.putCas(res);
	}
}
