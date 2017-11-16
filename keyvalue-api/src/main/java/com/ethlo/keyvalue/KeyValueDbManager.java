package com.ethlo.keyvalue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ethlo.keyvalue.keys.Key;

/**
 * 
 * @author mha
 */
public abstract class KeyValueDbManager<K extends Key, V, T extends KeyValueDb<K, V>> implements Closeable
{
	private Map<String, T> dbs = new HashMap<>();

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

	protected void close(String name) throws Exception
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