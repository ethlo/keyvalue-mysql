package com.ethlo.mycached;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Hex;
import org.springframework.util.DigestUtils;

/**
 * 
 * @author Morten Haraldsen
 */
public class LegacyMyCachedClientManagerImpl implements MyCachedClientManager
{
	private MysqlUtil mysqlUtil;
	private DataSource dataSource;
	
	public LegacyMyCachedClientManagerImpl(DataSource dataSource) throws IOException
	{
		this.dataSource = dataSource;
		this.mysqlUtil = new MysqlUtil(null, dataSource);		
	}
	
	@Override
	public MyCachedClient open(String tableName, boolean allowCreate)
	{
		if (tableName.length() > 64)
		{
			tableName = "md5_" + Hex.encodeHexString(DigestUtils.md5Digest(tableName.getBytes(StandardCharsets.UTF_8))).substring(10);
		}
		
		if (allowCreate)
		{
			this.mysqlUtil.createTable(tableName);
		}
		return new LegacyMyCachedClientImpl(tableName, dataSource);
	}

	@Override
	public List<String> list()
	{
		return null;
	}
}