package com.ethlo.keyvalue.hashmap;

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

import com.ethlo.keyvalue.KeyValueDbManager;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;

/**
 * @author mha
 */
public class HashmapKeyValueDbManager extends KeyValueDbManager<ByteArrayKey, byte[], CasKeyValueDb<ByteArrayKey, byte[], Long>>
{
    @Override
    protected CasKeyValueDb<ByteArrayKey, byte[], Long> createMainDb(String dbName, boolean create, KeyEncoder keyEncoder, DataCompressor dataCompressor)
    {
        return new HashmapKeyValueDb();
    }
}
