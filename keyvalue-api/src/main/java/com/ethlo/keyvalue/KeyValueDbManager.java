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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.keys.Key;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;

/**
 * 
 * @author mha
 */
public abstract class KeyValueDbManager<K extends Key, V, T extends KeyValueDb<K, V>> implements Closeable
{
	private final Map<String, T> dbs = new HashMap<>();

	protected abstract T createMainDb(String dbName, boolean create, KeyEncoder keyEncoder, DataCompressor dataCompressor);
	
	public T getDb(String dbName, boolean create, KeyEncoder keyEncoder, DataCompressor dataCompressor)
	{
		T db = this.getOpenDb(dbName);
		if (db == null)
		{
			db = createMainDb(dbName, create, keyEncoder, dataCompressor);
			this.dbs.put(dbName, db);
		}
		return db;
	}
	
    private T getOpenDb(String name)
	{
		return this.dbs.get(name);
	}

	protected void close(String name)
	{
		final T db = this.getOpenDb(name);
		if (db != null)
		{
			db.close();
		}
	}
	
	@Override
	public void close()
	{
		for (T db : this.dbs.values())
		{
			db.close();
		}
	}

	public final List<String> listOpen()
	{
		return new ArrayList<>(this.dbs.keySet());
	}
}
