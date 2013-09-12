package com.ethlo.mycached;


import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Base64;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.BatchCasKeyValueDb;
import com.ethlo.keyvalue.BatchCasWriteWrapper;
import com.ethlo.keyvalue.CasHolder;

/**
 * Works by using standard SQL for handling data operations instead of the MySql-MemCached interface
 * 
 * @author Morten Haraldsen
 */
public class LegacyMyCachedClientImpl implements BatchCasKeyValueDb<ByteBuffer, byte[], Long>
{
	private JdbcTemplate tpl;
	
	private final String getSql;
	private final String getCasSql;
	private final String insertSql;
	private final String replaceSql;
	private final String replaceCasSql;
	private final String deleteSql;
	private final String clearSql;
	private RowMapper<byte[]> rowMapper;
	private RowMapper<CasHolder<ByteBuffer, byte[], Long>> casRowMapper;
	
	public LegacyMyCachedClientImpl(String tableName, DataSource dataSource)
	{
		Assert.hasLength(tableName, "tableName cannot be null");
		Assert.notNull(dataSource, "dataSource cannot be null");
		this.tpl = new JdbcTemplate(dataSource);
		
		this.getSql = "SELECT mvalue FROM " + tableName + " WHERE mkey = ?";
		this.getCasSql = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey = ?";
		this.replaceSql = "REPLACE INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, COALESCE(0, cas_column + 1))";
		this.insertSql = "INSERT INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, ?)";
		this.replaceCasSql = "UPDATE " + tableName + " SET mvalue = ?, cas_column = cas_column + 1 WHERE mkey = ? AND cas_column = ?";
		this.deleteSql = "DELETE FROM " + tableName + " WHERE key = ?";
		this.clearSql = "DELETE FROM " + tableName;
		
		this.rowMapper = new RowMapper<byte[]>()
		{
			@Override
			public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				return CompressionUtil.uncompress(rs.getBytes(1));
			}			
		};
		
		this.casRowMapper = new RowMapper<CasHolder<ByteBuffer, byte[], Long>>()
		{
			@Override
			public CasHolder<ByteBuffer, byte[], Long> mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				final byte[] key = Base64.decodeBase64(rs.getString(1));
				final byte[] value = CompressionUtil.uncompress(rs.getBytes(2));
				final long cas = rs.getLong(3);
				return new CasHolder<ByteBuffer, byte[], Long>(cas, ByteBuffer.wrap(key), value);
			}			
		};
	}
	
	@Override
	public byte[] get(final ByteBuffer key)
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
		return DataAccessUtils.singleResult(tpl.query(getPsc, rowMapper));
	}

	private String getKey(ByteBuffer key)
	{
		return Base64.encodeBase64String(key.array());
	}
	
	@Override
	public void put(final ByteBuffer key, final byte[] value)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(replaceSql);
		        ps.setString(1, getKey(key)); 
		        ps.setBytes(2, CompressionUtil.compress(value));
		        return ps;
		    }
		};
		tpl.update(creator);
	}

	@Override
	public void delete(ByteBuffer key)
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
	public CasHolder<ByteBuffer, byte[], Long> getCas(final ByteBuffer key)
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
	public void putCas(final CasHolder<ByteBuffer, byte[], Long> cas)
	{
		if (cas.getCasValue() != null)
		{
			updateWithNonNullCasValue(cas);
		}
		else
		{
			insertNewDueToNullCasValue(cas);
		}
	}

	private void insertNewDueToNullCasValue(final CasHolder<ByteBuffer, byte[], Long> cas)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(insertSql);
		        ps.setString(1, getKey(cas.getKey())); 
		        ps.setBytes(2, CompressionUtil.compress(cas.getValue()));
		        ps.setLong(3, 0L);
		        return ps;
		    }
		};
		tpl.update(creator);
	}

	private void updateWithNonNullCasValue(final CasHolder<ByteBuffer, byte[], Long> cas)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		    	final String strKey = getKey(cas.getKey());
		    	final long casValue = cas.getCasValue();
		        final PreparedStatement ps = con.prepareStatement(replaceCasSql);
		        ps.setBytes(1, CompressionUtil.compress(cas.getValue()));
		        ps.setString(2, strKey);
		        ps.setLong(3, casValue);
		        return ps;
		    }
		};
		final int rowsChanged = tpl.update(creator);
		if (rowsChanged != 1)
		{
			throw new OptimisticLockingFailureException("Cannot update data due to concurrent modification");
		}
	}

	@Override
	public void flush(final BatchCasWriteWrapper<ByteBuffer, byte[], Long> batch)
	{
		for (CasHolder<ByteBuffer, byte[], Long> entry : batch.data())
		{
			putCas(entry);
		}
	}
}