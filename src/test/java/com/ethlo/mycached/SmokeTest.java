package com.ethlo.mycached;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.util.StopWatch;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath:/mycached-testcontext.xml"})
@TestExecutionListeners(listeners={DependencyInjectionTestExecutionListener.class, DirtiesContextTestExecutionListener.class})
public class SmokeTest
{
	@Resource
	private MyCachedClientManager clientManager;
	
	public void putAndGetCompare() throws SQLException
	{
		final String dbName = "someTestData";
		final MyCachedClient client = clientManager.open(dbName, true);
		final byte[] keyBytes = new byte[]{0,1,2,3,4,5,6,7};
		final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
		client.put(keyBytes, valueBytes);
		final byte[] retVal = client.get(keyBytes);
		Assert.assertArrayEquals(valueBytes, retVal);
	}
	
	@Test
	@Ignore
	public void performanceTest() throws SQLException
	{
		final String dbName = "someTestData";
		final MyCachedClient client = clientManager.open(dbName, true);
		final byte[] keyBytes = new byte[]{0,1,2,3,4,5,6,7};
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
