package com.ethlo.mycached;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.BatchCasKeyValueDb;
import com.ethlo.keyvalue.CasHolder;
import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.MutatingKeyValueDb;
import com.ethlo.keyvalue.SeekableIterator;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

/**
 * Works by using standard SQL for handling data operations instead of the MySql-MemCached interface
 * 
 * @author Morten Haraldsen
 */
public class LegacyMyCachedClientImpl implements 
	MutatingKeyValueDb<ByteArrayKey, byte[]>,
	IterableKeyValueDb<ByteArrayKey, byte[]>,
	BatchCasKeyValueDb<ByteArrayKey, byte[], Long>
{
	private JdbcTemplate tpl;
	
	private boolean useCompression = true;
	
	private final String getSql;
	private final String getCasSql;
	private final String getCasSqlPrefix;
	private final String insertSql;
	private final String replaceSql;
	private final String replaceCasSql;
	private final String deleteSql;
	private final String clearSql;
	private final RowMapper<byte[]> rowMapper;
	private final RowMapper<CasHolder<ByteArrayKey, byte[], Long>> casRowMapper;
	
	public LegacyMyCachedClientImpl(String tableName, DataSource dataSource, boolean useCompression)
	{
		this.useCompression = useCompression;
		
		Assert.hasLength(tableName, "tableName cannot be null");
		Assert.notNull(dataSource, "dataSource cannot be null");
		this.tpl = new JdbcTemplate(dataSource);
		
		this.getSql = "SELECT mvalue FROM " + tableName + " WHERE mkey = ?";
		this.getCasSql = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey = ?";
		this.getCasSqlPrefix = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey LIKE ?";
		this.replaceSql = "REPLACE INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, COALESCE(0, cas_column + 1))";
		this.insertSql = "INSERT INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, ?)";
		this.replaceCasSql = "UPDATE " + tableName + " SET mvalue = ?, cas_column = cas_column + 1 WHERE mkey = ? AND cas_column = ?";
		this.deleteSql = "DELETE FROM " + tableName + " WHERE mkey = ?";
		this.clearSql = "DELETE FROM " + tableName;
		
		this.rowMapper = new RowMapper<byte[]>()
		{
			@Override
			public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				final byte[] uncompressed = rs.getBytes(1);
				return useCompression ? CompressionUtil.uncompress(uncompressed) : uncompressed;
			}			
		};
		
		this.casRowMapper = new RowMapper<CasHolder<ByteArrayKey, byte[], Long>>()
		{
			@Override
			public CasHolder<ByteArrayKey, byte[], Long> mapRow(ResultSet rs, int rowNum) throws SQLException
			{
				final ByteArrayKey key = toKey(rs.getString(1));
				final byte[] uncompressed = rs.getBytes(2);
				final byte[] value = useCompression ? CompressionUtil.uncompress(uncompressed) : uncompressed;
				final long cas = rs.getLong(3);
				return new CasHolder<ByteArrayKey, byte[], Long>(cas, key, value);
			}
		};
	}
	
	@Override
	public byte[] get(final ByteArrayKey key)
	{
		final PreparedStatementCreator getPsc = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(getSql);
		        final String strKey = fromKey(key);
		        ps.setString(1, strKey);  
		        return ps;
		    }
		};
		return DataAccessUtils.singleResult(tpl.query(getPsc, rowMapper));
	}

    private ByteArrayKey toKey(String string)
    {
        try
        {
            return new ByteArrayKey(Hex.decodeHex(string.toCharArray()));
        }
        catch (DecoderException exc)
        {
            throw Throwables.propagate(exc);
        }
        //return new ByteArrayKey(Base64.decodeBase64(string));
    }
	
	private String fromKey(ByteArrayKey key)
	{
	    return Hex.encodeHexString(key.getByteArray());
	    //return Base64.encodeBase64String(key.getByteArray());
	}
	
	@Override
	public void put(final ByteArrayKey key, final byte[] value)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(replaceSql);
		        final String strKey = fromKey(key);
		        ps.setString(1, strKey); 
		        ps.setBytes(2, useCompression ? CompressionUtil.compress(value) : value);
		        return ps;
		    }
		};
		tpl.update(creator);
	}

	@Override
	public void delete(ByteArrayKey key)
	{
		tpl.update(deleteSql, fromKey(key));
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
	public CasHolder<ByteArrayKey, byte[], Long> getCas(final ByteArrayKey key)
	{
		final PreparedStatementCreator getCasPsc = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(getCasSql);
		        final String strKey = fromKey(key);
		        ps.setString(1, strKey);  
		        return ps;
		    }
		};
		return DataAccessUtils.singleResult(tpl.query(getCasPsc, casRowMapper));
	}

	@Override
	public void putCas(final CasHolder<ByteArrayKey, byte[], Long> cas)
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

	private void insertNewDueToNullCasValue(final CasHolder<ByteArrayKey, byte[], Long> cas)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		        final PreparedStatement ps = con.prepareStatement(insertSql);
		        final String strKey = fromKey(cas.getKey());
		        ps.setString(1, strKey); 
		        ps.setBytes(2, useCompression ? CompressionUtil.compress(cas.getValue()) : cas.getValue());
		        ps.setLong(3, 0L);
		        return ps;
		    }
		};
		
		try
		{
			tpl.update(creator);
		}
		catch (DuplicateKeyException exc)
		{
			throw new OptimisticLockingFailureException("Cannot update " + cas.getKey(), exc);
		}
	}

	private void updateWithNonNullCasValue(final CasHolder<ByteArrayKey, byte[], Long> cas)
	{
		final PreparedStatementCreator creator = new PreparedStatementCreator()
		{
		    @Override
		    public PreparedStatement createPreparedStatement(Connection con) throws SQLException
		    {
		    	final String strKey = fromKey(cas.getKey());
		    	final long casValue = cas.getCasValue();
		        final PreparedStatement ps = con.prepareStatement(replaceCasSql);
		        ps.setBytes(1, useCompression ? CompressionUtil.compress(cas.getValue()) : cas.getValue());
		        ps.setString(2, strKey);
		        ps.setLong(3, casValue);
		        return ps;
		    }
		};
		final int rowsChanged = tpl.update(creator);
		if (rowsChanged == 0)
		{
			throw new OptimisticLockingFailureException("Cannot update data for key " + cas.getKey() + " due to concurrent modification. Details: Attempted CAS value=" + cas.getCasValue());
		}
	}

	@Override
	public void putBatch(final List<CasHolder<ByteArrayKey, byte[], Long>> casList)
	{
		// Hard to solve in any more efficient way
		for (CasHolder<ByteArrayKey, byte[], Long> cas : casList)
		{
			putCas(cas);
		}
	}

	@Override
	public void mutate(ByteArrayKey key, Function<byte[], byte[]> mutator)
	{
		final CasHolder<ByteArrayKey, byte[], Long> cas = this.getCas(key);
		final byte[] result = mutator.apply(cas != null ? Arrays.copyOf(cas.getValue(), cas.getValue().length) : null);
		
		if (cas != null)
		{
			this.putCas(cas.setValue(result));
		}
		else
		{
			this.putCas(new CasHolder<ByteArrayKey, byte[], Long>(null, key, result));
		}
	}
	
	public void useCompression(boolean compress)
	{
		this.useCompression = compress;
	}
	
	public boolean useCompression()
	{
		return this.useCompression;
	}

    @Override
    public SeekableIterator<ByteArrayKey, byte[]> iterator()
    {
        return new JdbcSeekableIterator();
    }
    
    private class JdbcSeekableIterator extends AbstractIterator<Entry<ByteArrayKey, byte[]>> implements SeekableIterator<ByteArrayKey, byte[]>
    {
        private ResultSet rs = null;
        private Connection connection;
        private PreparedStatement ps;
        
        @Override
        protected Entry<ByteArrayKey, byte[]> computeNext()
        {
            try
            {
                if (! rs.isAfterLast())
                {
                    final CasHolder<ByteArrayKey, byte[], Long> res = casRowMapper.mapRow(rs, rs.getRow());
                    rs.next();
                    return new AbstractMap.SimpleEntry<ByteArrayKey, byte[]>(res.getKey(), res.getValue());
                }
                return endOfData();
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }

        @Override
        public void close()
        {
            if (rs != null)
            {
                try
                {
                    rs.close();
                }
                catch (SQLException exc)
                {
                    throw new DataAccessResourceFailureException(exc.getMessage(), exc);
                }
            }
            
            if (ps != null)
            {
                try
                {
                    ps.close();
                }
                catch (SQLException exc)
                {
                    throw new DataAccessResourceFailureException(exc.getMessage(), exc);
                }
            }
            
            if (connection != null)
            {
                try
                {
                    connection.close();
                }
                catch (SQLException exc)
                {
                    throw new DataAccessResourceFailureException(exc.getMessage(), exc);
                }
            }
        }

        @Override
        public boolean hasPrevious()
        {
            try
            {
                final boolean hasPrevious = rs.previous();
                if (hasPrevious)
                {
                    rs.next();
                }
                return hasPrevious;
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }

        @Override
        public Entry<ByteArrayKey, byte[]> previous()
        {
            try
            {
                if (rs.previous())
                {
                    final CasHolder<ByteArrayKey, byte[], Long> res = casRowMapper.mapRow(rs, rs.getRow());
                    return new AbstractMap.SimpleEntry<ByteArrayKey, byte[]>(res.getKey(), res.getValue());
                }
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
            throw new EmptyResultDataAccessException("No previous result", 1);
        }

        @Override
        public void seekTo(ByteArrayKey key)
        {
            final PreparedStatementCreator getCasPsc = new PreparedStatementCreator()
            {
                @Override
                public PreparedStatement createPreparedStatement(Connection con) throws SQLException
                {
                    final PreparedStatement ps = con.prepareStatement(getCasSqlPrefix, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                    final String strKey = fromKey(key);
                    ps.setString(1, strKey + "%");  
                    return ps;
                }
            };
            
            try
            {
                if (this.connection == null)
                {
                    this.connection = tpl.getDataSource().getConnection();
                    this.ps = getCasPsc.createPreparedStatement(connection);
                }
                this.rs = ps.executeQuery();
                if (! this.rs.first())
                {
                    throw new EmptyResultDataAccessException(1);
                }
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }

        @Override
        public void seekToFirst()
        {
            if (rs != null)
            {
                try
                {
                    rs.first();
                }
                catch (SQLException exc)
                {
                    throw new DataAccessResourceFailureException(exc.getMessage(), exc);
                }
            }
        }

        @Override
        public void seekToLast()
        {
            try
            {
                rs.last();
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }
    }
}