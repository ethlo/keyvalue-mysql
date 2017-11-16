package com.ethlo.keyvalue.hashmap;

import com.ethlo.keyvalue.CasKeyValueDb;
import com.ethlo.keyvalue.DataCompressor;
import com.ethlo.keyvalue.KeyEncoder;
import com.ethlo.keyvalue.KeyValueDbManager;
import com.ethlo.keyvalue.keys.ByteArrayKey;

/**
 * 
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
