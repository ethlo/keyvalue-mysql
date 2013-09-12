package com.ethlo.mycached;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import com.ethlo.keyvalue.KeyValueDbManager;
import com.ethlo.mycached.MysqlJdbcConnector.MyCachedConfig;

/**
 * 
 * @author Morten Haraldsen
 */
public class MyCachedClientManagerImpl extends KeyValueDbManager<ByteBuffer, byte[], CasKeyValueDb<ByteBuffer,byte[], Long>>
{
	private MemcachedClient mcc;
	private MysqlUtil mysqlUtil;
	private String schemaName;
	
	public MyCachedClientManagerImpl(MyCachedConfig cfg) throws IOException
	{
		final DataSource dataSource = new MysqlJdbcConnector(cfg);
		this.mysqlUtil = new MysqlUtil(cfg.getSchemaName(), dataSource);
		
		final MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(cfg.getHost() + ":" + cfg.getMemCachedPort()));
		builder.setCommandFactory(new BinaryCommandFactory());
		this.mcc = builder.build();
		this.schemaName = cfg.getSchemaName();
	}
	
	@Override
	public CasKeyValueDb<ByteBuffer,byte[], Long> createMainDb(String tableName, boolean allowCreate)
	{
		final String hash = "md5_" + Hex.encodeHexString(DigestUtils.md5Digest(tableName.getBytes(StandardCharsets.UTF_8))).substring(10);
		this.mysqlUtil.setup(hash, allowCreate);
		return new MyCachedClientImpl(schemaName, tableName, mcc);
	}
}