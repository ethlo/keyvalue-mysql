package com.ethlo.keyvalue;

/*-
 * #%L
 * Key/Value API
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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;

/**
 * @author Morten Haraldsen
 */
public class KeyValueDbTest extends AbstractKeyValueDbTest
{
    @Autowired
    private KeyValueDbManager<ByteArrayKey, byte[], KeyValueDb<ByteArrayKey, byte[]>> kvDbManager;

    private KeyValueDb<ByteArrayKey, byte[]> db;

    @Before
    public void fetchDb()
    {
        this.db = this.kvDbManager.getDb("test", true, new HexKeyEncoder(), new NopDataCompressor());
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
