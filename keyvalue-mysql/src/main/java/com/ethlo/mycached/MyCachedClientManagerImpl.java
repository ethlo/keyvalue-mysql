package com.ethlo.mycached;

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