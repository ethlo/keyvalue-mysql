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

import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.apache.commons.codec.binary.Hex;
import org.springframework.util.DigestUtils;

import com.ethlo.keyvalue.CasKeyValueDb;
import com.ethlo.keyvalue.DataCompressor;
import com.ethlo.keyvalue.KeyEncoder;
import com.ethlo.keyvalue.KeyValueDbManager;
import com.ethlo.keyvalue.keys.ByteArrayKey;

/**
 * 
 * @author Morten Haraldsen
 */
public class MyCachedClientManagerImpl extends KeyValueDbManager<ByteArrayKey, byte[], CasKeyValueDb<ByteArrayKey,byte[], Long>>
{
	private MemcachedClient mcc;
	private MysqlUtil mysqlUtil;
	private String schemaName;
	
	public MyCachedClientManagerImpl(final String schemaName, final String host, int memCachedPort, DataSource dataSource) throws IOException
	{
		this.mysqlUtil = new MysqlUtil(schemaName, dataSource);
		
		final MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(host + ":" + memCachedPort));
		builder.setCommandFactory(new BinaryCommandFactory());
		this.mcc = builder.build();
		this.schemaName = schemaName;
	}
	
	@Override
	public CasKeyValueDb<ByteArrayKey,byte[], Long> createMainDb(String tableName, boolean allowCreate, KeyEncoder keyEncoder, DataCompressor dataCompressor)
	{
		final String hash = "md5_" + Hex.encodeHexString(DigestUtils.md5Digest(tableName.getBytes(StandardCharsets.UTF_8))).substring(10);
		this.mysqlUtil.setup(hash, allowCreate);
		return new MyCachedClientImpl(schemaName, tableName, mcc);
	}
}
