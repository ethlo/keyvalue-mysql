package com.ethlo.mycached;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * 
 * @author Morten Haraldsen
 */
public class MysqlJdbcConnector implements Closeable, DataSource
{
	private BoneCP connectionPool;
	
	public MysqlJdbcConnector(MyCachedConfig cfg)
	{
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
		 	BoneCPConfig config = new BoneCPConfig();
		 	config.setJdbcUrl("jdbc:mysql://" + cfg.getHost() + ":" + cfg.getJdbcPort() + "/" + cfg.getSchemaName());
			config.setUsername(cfg.getJdbcUsername());
			config.setPassword(cfg.getJdbcPassword());
			config.setMaxConnectionsPerPartition(cfg.getPoolSize());
			this.connectionPool = new BoneCP(config);
		}
		catch (ClassNotFoundException | SQLException exc)
		{
			throw new RuntimeException(exc);
		}
	}

	@Override
	public void close() throws IOException
	{
		connectionPool.shutdown();
	}
	
	@Override
	public Connection getConnection() throws SQLException
	{
		return connectionPool.getConnection();
	}
	
	public static class MyCachedConfig
	{
		private String dbName = "memcached_data";
		private String host = "localhost";
		private int jdbcPort = 3306;
		private int memCachedPort = 11211;
		private String jdbcUsername = "sa";
		private String jdbcPassword = "";
		private int poolSize = 25;
		
		public String getSchemaName()
		{
			return dbName;
		}
		
		public void setDbName(String dbName)
		{
			this.dbName = dbName;
		}
		
		public String getHost()
		{
			return host;
		}
		
		public void setHost(String host)
		{
			this.host = host;
		}
		
		public int getJdbcPort()
		{
			return jdbcPort;
		}
		
		public void setJdbcPort(int jdbcPort)
		{
			this.jdbcPort = jdbcPort;
		}
		
		public int getMemCachedPort()
		{
			return memCachedPort;
		}
		
		public void setMemCachedPort(int memCachedPort)
		{
			this.memCachedPort = memCachedPort;
		}
		
		public String getJdbcUsername()
		{
			return jdbcUsername;
		}
		
		public void setJdbcUsername(String username)
		{
			this.jdbcUsername = username;
		}
		
		public String getJdbcPassword()
		{
			return jdbcPassword;
		}
		
		public void setJdbcPassword(String password)
		{
			this.jdbcPassword = password;
		}
		
		public int getPoolSize()
		{
			return poolSize;
		}
		
		public void setPoolSize(int poolSize)
		{
			this.poolSize = poolSize;
		}
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLogWriter(PrintWriter arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLoginTimeout(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Connection getConnection(String username, String password)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
}
