package com.ethlo.mycached;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Base64;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.CasHolder;

/**
 * Works by using standard SQL for handling data operations instead of the MySql-MemCached interface
 * 
 * @author mha
 */
public class LegacyMyCachedClientImpl implements MyCachedClient
{
	private JdbcTemplate tpl;
	
	private final String getSql;
	private final String getCasSql;
	private final String putSql;
	private final String putCasSql;
	private final String deleteSql;
	private final String clearSql;
	private RowMapper<byte[]> rowMapper;
	private RowMapper<CasHolder<byte[], byte[], Long>> casRowMapper;
	
	public LegacyMyCachedClientImpl(String tableName, DataSource dataSource)
	{
		Assert.hasLength(tableName, "tableName cannot be null");
		Assert.notNull(dataSource, "dataSource cannot be null");
		this.tpl = new JdbcTemplate(dataSource);
		
		this.getSql = "SELECT mvalue FROM " + tableName + " WHERE mkey = ?";
		this.getCasSql = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey = ?";
		this.putSql = "REPLACE INTO " + tableName + " (mkey, mvalue) VALUES(?, ?)";
		this.putCasSql = "REPLACE INTO " + tableName + " (mkey, mvalue) VALUES(?, ?) WHERE cas_column = ?";
		this.deleteSql = "DELETE FROM " + tableName + " WHERE key = ?";
		this.clearSql = "DELETE FROM " + tableName;
		
		this.rowMapper = new RowMapper<byte[]>()
		{
			@Override
			public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				return rs.getBytes(1);
			}			
		};
		
		this.casRowMapper = new RowMapper<CasHolder<byte[], byte[], Long>>()
		{
			@Override
			public CasHolder<byte[], byte[], Long> mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				final byte[] key = Base64.decodeBase64(rs.getString(1));
				final byte[] value = rs.getBytes(2);
				final long cas = rs.getLong(3);
				return new CasHolder<byte[], byte[], Long>(cas, key, value);
			}			
		};
	}
	
	@Override
	public byte[] get(final byte[] key)
	{
		final PreparedStatementCreator getPsc = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(getSql);
		        ps.setString(1, getKey(key));  
		        return ps;
		    }
		};
		final byte[] data = DataAccessUtils.singleResult(tpl.query(getPsc, rowMapper));
		return CompressionUtil.uncompress(data);
	}

	private String getKey(byte[] key)
	{
		return Base64.encodeBase64String(key);
	}

	@Override
	public void put(final byte[] key, final byte[] value)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(putSql);
		        ps.setString(1, getKey(key)); 
		        ps.setBytes(2, CompressionUtil.compress(value)); 
		        return ps;
		    }
		};
		tpl.update(creator);
	}

	@Override
	public void delete(byte[] key)
	{
		tpl.update(deleteSql, Collections.singletonMap("key", getKey(key)));
	}

	@Override
	public void clear()
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        return con.prepareStatement(clearSql);
		    }
		};
		tpl.update(creator);
	}

	@Override
	public void close()
	{
		
	}

	@Override
	public CasHolder<byte[], byte[], Long> getCas(final byte[] key)
	{
		final PreparedStatementCreator getCasPsc = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(getCasSql);
		        ps.setString(1, getKey(key));  
		        return ps;
		    }
		};
		return DataAccessUtils.singleResult(tpl.query(getCasPsc, casRowMapper));
	}

	@Override
	public void putCas(final CasHolder<byte[], byte[], Long> cas)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(putCasSql);
		        ps.setString(1, getKey(cas.getKey())); 
		        ps.setBytes(2, CompressionUtil.compress(cas.getValue()));
		        ps.setLong(3, cas.getCasValue());
		        return ps;
		    }
		};
		tpl.update(creator);
	}
}