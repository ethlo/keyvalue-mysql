package com.ethlo.mycached;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.util.StopWatch;

import com.ethlo.keyvalue.CasHolder;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/mycached-testcontext.xml"})
@TestExecutionListeners(listeners={DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class})
public class SmokeTest
{
	@Resource
	private MyCachedClientManager clientManager;
	
	private MyCachedClient client;
	
	
	@Before
	public void setup()
	{
		final String dbName = "someTestData";
		this.client = clientManager.open(dbName, true);
	}
	
	@Test
	public void putAndGetCompare() throws SQLException
	{
		final ByteBuffer keyBytes = ByteBuffer.wrap(new byte[]{0,1,2,3,4,5,6,7});
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		client.put(keyBytes, valueBytes);
		final byte[] retVal = client.get(keyBytes);
		Assert.assertArrayEquals(valueBytes, retVal);
	}
	
	@Test
	public void testCas() throws SQLException
	{
		final ByteBuffer keyBytes = ByteBuffer.wrap(new byte[]{4,5,6,7,9,9});
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsmakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
		
		client.put(keyBytes, valueBytes);
		
		final CasHolder<ByteBuffer, byte[], Long> res = client.getCas(keyBytes);
		Assert.assertEquals(Long.valueOf(0L), res.getCasValue());
		Assert.assertArrayEquals(valueBytes, res.getValue());
		
		res.setValue(valueBytesUpdated);
		client.putCas(res);
	}
	
	@Test
	@Ignore
	public void performanceTest() throws SQLException
	{
		final String dbName = "someTestData";
		final MyCachedClient client = clientManager.open(dbName, true);
		final ByteBuffer keyBytes = ByteBuffer.wrap(new byte[]{0,1,2,3,4,5,6,7});
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		
		final int tests = 100000;
		final StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		for (int i = 0; i < tests; i++)
		{
			client.put(keyBytes, valueBytes);
			final byte[] retVal = client.get(keyBytes);
			Assert.assertArrayEquals(valueBytes, retVal);
		}
		stopWatch.stop();
		System.out.println(stopWatch);
	}
}
