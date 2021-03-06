package com.ethlo.keyvalue.mysql;

/*-
 * #%L
 * Key/value MySQL implementation
 * %%
 * Copyright (C) 2013 - 2020 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.springframework.dao.OptimisticLockingFailureException;

import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.google.common.base.Function;
import com.google.common.primitives.Ints;

public class ConcurrentCasStressTest extends AbstractTest
{
    @Test
    public void casStressTest() throws Exception
    {
        final int threadCount = 100;
        final int iterations = 200;
        final List<Callable<Void>> threadArr = new ArrayList<>(threadCount);
        final AtomicInteger failed = new AtomicInteger();
        for (int threadIdx = 0; threadIdx < threadCount; threadIdx++)
        {
            threadArr.add(() ->
            {
                for (int j = 0; j < iterations; j++)
                {
                    final ByteArrayKey key = new ByteArrayKey(Ints.toByteArray(j));

                    boolean failing = true;
                    while (failing)
                    {
                        try
                        {
                            db.mutate(key, (Function<byte[], byte[]>) input ->
                            {
                                final int curCount = input != null ? Ints.fromByteArray(input) : 0;

                                // Increment by 1
                                return Ints.toByteArray(curCount + 1);
                            });
                            failing = false;
                        }
                        catch (OptimisticLockingFailureException exc)
                        {
                            failed.incrementAndGet();
                            failing = true;
                        }
                    }
                }
                return null;
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
            final int count = Ints.fromByteArray(db.get(key));
            assertThat(count).isEqualTo(threadCount);
        }
        System.out.println("Failed attempts " + failed.get());
    }

    @Test
    public void casStressTestSameKey() throws Exception
    {
        final int threadCount = 25;
        final int iterations = 100;
        final List<Callable<Void>> threadArr = new ArrayList<>(threadCount);
        final ByteArrayKey key = new ByteArrayKey(Ints.toByteArray((int) (Math.random() * Integer.MAX_VALUE)));
        final AtomicInteger failed = new AtomicInteger();
        for (int threadIdx = 0; threadIdx < threadCount; threadIdx++)
        {
            threadArr.add(() -> {
                for (int j = 0; j < iterations; j++)
                {
                    boolean failing = true;
                    while (failing)
                    {
                        try
                        {
                            db.mutate(key, (Function<byte[], byte[]>) input ->
                            {
                                final int curCount = input != null ? Ints.fromByteArray(input) : 0;

                                // Increment by 1
                                return Ints.toByteArray(curCount + 1);
                            });

                            failing = false;
                        }
                        catch (OptimisticLockingFailureException exc)
                        {
                            failed.incrementAndGet();
                        }
                    }
                }
                return null;
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

        final int count = Ints.fromByteArray(db.get(key));
        System.out.println(count + " (failed attempts " + failed.get() + ")");
        assertThat(count).isEqualTo(threadCount * iterations);
    }

    @Override
    protected boolean useReplaceInto()
    {
        return false;
    }
}
