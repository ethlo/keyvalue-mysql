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

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ethlo.keyvalue.compression.NopDataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.HexKeyEncoder;

/**
 * @author Morten Haraldsen
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = TestCfg.class)
public abstract class AbstractKeyValueDbTest
{
    @Autowired
    private KeyValueDbManager kvDbManager;

    protected KeyValueDb<ByteArrayKey, byte[]> db;

    @Before
    public void fetchDb()
    {
        this.db = this.kvDbManager.getDb("test", true, new HexKeyEncoder(), new NopDataCompressor());
    }
}
