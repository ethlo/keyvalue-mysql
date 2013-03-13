package com.ethlo.mycached;

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
			ensureMapped(dbName);
		}
		else
		{
			throw new MyCachedIoException("The database table " + dbName + " does not exist and allowCreate is false", null);
		}
	}
	
	private void ensureMapped(String dbName)
	{
		if (! mappingExists(schemaName, dbName))
		{
			logger.info("No mapping for {} in database {}, adding mapping", dbName, schemaName);
			
			final Map<String, String> params = new TreeMap<>();
			params.put("name", schemaName + "_" + dbName);
			params.put("db_schema", schemaName);
			params.put("db_table", dbName);
			params.put("key_columns", "mkey");
			params.put("value_columns", "mvalue");
			params.put("flags", "flags");
			params.put("cas_column", "cas_column");
			params.put("expire_time_column", "expire_time_column");
			params.put("unique_idx_name_on_key", "PRIMARY");
			final String sql = 
				"INSERT INTO " + INNODB_MEMCACHE_DB_NAME + ".containers " +
				"(name, db_schema, db_table, key_columns, value_columns, flags, cas_column, expire_time_column, unique_idx_name_on_key)" +
				"VALUES(:name, :db_schema, :db_table, :key_columns, :value_columns, :flags, :cas_column, :expire_time_column, :unique_idx_name_on_key)";
			tpl.update(sql, params);
		}
	}
	
	private boolean mappingExists(String schemaName, String dbName)
	{
		final String sql = "SELECT * FROM " + INNODB_MEMCACHE_DB_NAME + ".containers WHERE db_schema=:db_schema AND db_table=:db_table";
		final Map<String, String> params = new TreeMap<>();
		params.put("db_schema", schemaName);
		params.put("db_table", dbName);
		return tpl.query(sql, params, new ResultSetExtractor<Boolean>()
		{
			@Override
			public Boolean extractData(ResultSet rs) throws SQLException, DataAccessException
			{
				return rs.next();
			}
		});
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

	private boolean tableExists(String dbName)
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
