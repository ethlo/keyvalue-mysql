package com.ethlo.mycached;

/*-
 * #%L
 * Key/value MySQL implementation
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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.Assert;

/**
 * 
 * @author mha
 */
public class MysqlUtil
{
	public static final String INNODB_MEMCACHE_DB_NAME = "innodb_memcache";
	public static final int TABLE_NAME_MAX_LENGTH = 64;
	
	private static final Logger logger = LoggerFactory.getLogger(MysqlUtil.class);	
	private DataSource dataSource;
	private NamedParameterJdbcTemplate tpl;
	private String schemaName;

	public MysqlUtil(String schemaName, DataSource dataSource)
	{
		this.dataSource = dataSource;
		this.tpl = new NamedParameterJdbcTemplate(dataSource);
		this.schemaName = schemaName;
	}

	public void setup(String dbName, boolean allowCreate)
	{
		Assert.isTrue(dbName.length() <= TABLE_NAME_MAX_LENGTH, "dbName " + dbName + " is too long. Maximum is " + TABLE_NAME_MAX_LENGTH + " characters");
		
		if(tableExists(dbName) || allowCreate)
		{
			createTable(dbName);
		}
		else
		{
			throw new MyCachedIoException("The database table " + dbName + " does not exist and allowCreate is false", null);
		}
	}

	public void createTable(String dbName)
	{
		Assert.hasLength(dbName, "dbName cannot be empty");
		tpl.update("CREATE TABLE IF NOT EXISTS " + dbName + "(" +
			"mkey VARBINARY(255) NOT NULL PRIMARY KEY, " +
			"mvalue MEDIUMBLOB NOT NULL, " +
			"cas_column INT NOT NULL, " +
			"expire_time_column INT, " +
			"flags INT) " +
			"ENGINE=INNODB", new TreeMap<String, String>());
	}

	boolean tableExists(String dbName)
	{
		try (final Connection c = this.dataSource.getConnection())
		{
			DatabaseMetaData md = c.getMetaData();
			ResultSet rs = md.getTables(null, null, "%", null);
			while (rs.next())
			{
				final String tableName = rs.getString(3);
				if (dbName.equals(tableName))
				{
					return true;
				}
			}
			return false;
	    }
		catch (SQLException e)
	    {
			throw new RuntimeException(e);
		}
	}

}
