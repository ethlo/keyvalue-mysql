package com.ethlo.mycached;

/*-
 * #%L
 * Key/value MySQL implementation
 * %%
 * Copyright (C) 2015 - 2018 Morten Haraldsen (ethlo)
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Hex;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.util.DigestUtils;

import com.ethlo.keyvalue.BatchCasKeyValueDb;
import com.ethlo.keyvalue.DataCompressor;
import com.ethlo.keyvalue.KeyEncoder;
import com.ethlo.keyvalue.KeyValueDbManager;
import com.ethlo.keyvalue.keys.ByteArrayKey;

/**
 * 
 * @author Morten Haraldsen
 */
public class LegacyMyCachedClientManagerImpl extends KeyValueDbManager<ByteArrayKey, byte[], BatchCasKeyValueDb<ByteArrayKey, byte[], Long>>
{
	private MysqlUtil mysqlUtil;
	private DataSource dataSource;
	
	public LegacyMyCachedClientManagerImpl(DataSource dataSource) throws IOException
	{
		this.dataSource = dataSource;
		this.mysqlUtil = new MysqlUtil(null, dataSource);		
	}
	
	@Override
	public BatchCasKeyValueDb<ByteArrayKey, byte[],Long> createMainDb(String tableName, boolean allowCreate, KeyEncoder keyEncoder, DataCompressor dataCompressor)
	{
		if (tableName.length() > 64)
		{
			tableName = "md5_" + Hex.encodeHexString(DigestUtils.md5Digest(tableName.getBytes(StandardCharsets.UTF_8))).substring(10);
		}
		
		if (! allowCreate && !this.mysqlUtil.tableExists(tableName))
		{
			throw new DataAccessResourceFailureException("No such database: " + tableName);
		}
		else
		{
			this.mysqlUtil.createTable(tableName);
		}
		return new LegacyMyCachedClientImpl(tableName, dataSource, keyEncoder, dataCompressor);
	}	
}
