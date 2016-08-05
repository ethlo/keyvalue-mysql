package com.ethlo.mycached;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import javax.annotation.Resource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.ethlo.keyvalue.CasHolder;
import com.ethlo.keyvalue.CasKeyValueDb;
import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.MutatingKeyValueDb;
import com.ethlo.keyvalue.SeekableIterator;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.google.common.base.Function;

public class SmokeTest extends AbstractTest
{
    @Resource
    private LegacyMyCachedClientManagerImpl clientManager;

    private CasKeyValueDb<ByteArrayKey, byte[], Long> client;

    @Before
    public void setup()
    {
        final String dbName = "someTestData";
        this.client = clientManager.createMainDb(dbName, true, keyEncoder, dataCompressor);
    }

    @Test
    public void putAndGetCompare() throws SQLException
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]
        { 0, 1, 2, 3, 4, 5, 6, 7 });
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        client.put(keyBytes, valueBytes);
        final byte[] retVal = client.get(keyBytes);
        Assert.assertArrayEquals(valueBytes, retVal);
    }

    @Test
    public void testCas() throws SQLException
    {
        final ByteArrayKey keyBytes = new ByteArrayKey(new byte[]{ 4, 5, 6, 7, 9, 9 });
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);
        client.put(keyBytes, valueBytes);
        
        final CasHolder<ByteArrayKey, byte[], Long> res = client.getCas(keyBytes);
        Assert.assertEquals(keyBytes, res.getKey());
        Assert.assertEquals(Long.valueOf(0L), res.getCasValue());
        Assert.assertArrayEquals(valueBytes, res.getValue());

        final byte[] valueBytesUpdated = "ThisIsTheDataToStoreSoLetsmakeItABitLongerAndEvenUpdated".getBytes(StandardCharsets.UTF_8);
        res.setValue(valueBytesUpdated);
        client.putCas(res);
    }

    @Test
    public void testIterate() throws SQLException
    {
        final ByteArrayKey keyBytes0 = new ByteArrayKey(new byte[] { 0, 0 });
        final ByteArrayKey keyBytes1 = new ByteArrayKey(new byte[] { 1, 0 });
        final ByteArrayKey keyBytes2 = new ByteArrayKey(new byte[] { 1, 1 });
        final ByteArrayKey keyBytes3 = new ByteArrayKey(new byte[] { 1, 2 });
        final ByteArrayKey keyBytes4 = new ByteArrayKey(new byte[] { 2, 0 });
        final byte[] valueBytes = "ThisIsTheDataToStoreSoLetsmakeItABitLonger".getBytes(StandardCharsets.UTF_8);

        client.put(keyBytes0, valueBytes);
        client.put(keyBytes1, valueBytes);
        client.put(keyBytes2, valueBytes);
        client.put(keyBytes3, valueBytes);
        client.put(keyBytes4, valueBytes);

        IterableKeyValueDb<ByteArrayKey, byte[]> idb = (IterableKeyValueDb<ByteArrayKey, byte[]>) client;
        int count = 0;
        try (final SeekableIterator<ByteArrayKey, byte[]> iter = idb.iterator())
        {
            final ByteArrayKey prefixKey = new ByteArrayKey(new byte[]{1});
            iter.seekTo(prefixKey);
            while (iter.hasNext())
            {
                System.out.println(iter.next());
                count++;
            }
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testMutate() throws SQLException
    {
        final ByteArrayKey key = new ByteArrayKey(new byte[]{6});
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
