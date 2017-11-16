package com.ethlo.mycached;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Resource;

import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.OptimisticLockingFailureException;

import com.ethlo.keyvalue.MutatingKeyValueDb;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.google.common.base.Function;
import com.google.common.primitives.Ints;

public class ConcurrentCasStressTest extends AbstractTest
{
	@Resource
	private LegacyMyCachedClientManagerImpl clientManager;
	
	private MutatingKeyValueDb<ByteArrayKey, byte[]> client;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setup()
	{
		final String dbName = "_test";
		this.client = (MutatingKeyValueDb<ByteArrayKey, byte[]>) clientManager.createMainDb(dbName, true, keyEncoder, dataCompressor);
	}
	
	@Test
	public void casStressTest() throws Exception
	{
		final int threadCount = 200;
		final int iterations = 500;
		final List<Callable<Void>> threadArr = new ArrayList<Callable<Void>>(threadCount);
		final AtomicInteger failed = new AtomicInteger();
		for (int threadIdx = 0; threadIdx < threadCount; threadIdx++)
		{
			threadArr.add(new Callable<Void>()
			{
				@Override
				public Void call() throws Exception
				{
					for (int j = 0; j < iterations; j++)
					{
						final ByteArrayKey key = new ByteArrayKey(Ints.toByteArray(j));
						
						boolean failing = true;
						while (failing)
						{
							try
							{
								client.mutate(key, new Function<byte[], byte[]>()
								{
									@Override
									public byte[] apply(byte[] input)
									{
										final int curCount = input != null ? Ints.fromByteArray(input) : 0;
		
										// Increment by 1
										return Ints.toByteArray(curCount + 1);
									}
								});
								failing = false;
							}
							catch (OptimisticLockingFailureException exc)
							{
								failing = true;
								failed.incrementAndGet();
							}
						}
					}
					return null;
				}
			});
		}
			
		// Launch threads
		final ExecutorService exec = Executors.newFixedThreadPool(threadArr.size());
		final List<Future<Void>> result = exec.invokeAll(threadArr);
		
		// Check results
		for (Future<Void> res : result)
		{
			res.get();
		}
		
		// Check data is correct
		for (int i = 0; i < iterations; i++)
		{
			final ByteArrayKey key = new ByteArrayKey(Ints.toByteArray(i));
			final int count = Ints.fromByteArray(client.get(key));
			assertThat(count).isEqualTo(threadCount);
		}
		System.out.println("Failed attempts " + failed.get());
	}
	
	@Test
	public void casStressTestSameKey() throws Exception
	{
		final int threadCount = 25;
		final int iterations = 100;
		final List<Callable<Void>> threadArr = new ArrayList<Callable<Void>>(threadCount);
		final ByteArrayKey key = new ByteArrayKey(Ints.toByteArray((int) (Math.random() * Integer.MAX_VALUE)));
		final AtomicInteger failed = new AtomicInteger();
		for (int threadIdx = 0; threadIdx < threadCount; threadIdx++)
		{
			threadArr.add(new Callable<Void>()
			{				
				@Override
				public Void call() throws Exception
				{
					for (int j = 0; j < iterations; j++)
					{
						boolean failing = true;
						while (failing)
						{
							try
							{
								client.mutate(key, new Function<byte[], byte[]>()
								{
									@Override
									public byte[] apply(byte[] input)
									{
										final int curCount = input != null ? Ints.fromByteArray(input) : 0;
										
										// Increment by 1
										return Ints.toByteArray(curCount + 1);
									}
								});

								failing = false;
							}
							catch (OptimisticLockingFailureException exc)
							{
								failing = true;
								failed.incrementAndGet();
							}
						}
					}
					return null;
				}
			});
		}
		
		// Launch threads
		final ExecutorService exec = Executors.newFixedThreadPool(threadArr.size());
		final List<Future<Void>> result = exec.invokeAll(threadArr);
		
		// Check results
		for (Future<Void> res : result)
		{
			res.get();
		}
		
		final int count = Ints.fromByteArray(client.get(key));
		System.out.println(count + " (failed attempts " + failed.get() + ")");
		assertThat(count).isEqualTo(threadCount * iterations);
	}
}	
