package com.ethlo.keyvalue.mysql;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.BatchKeyValueDb;
import com.ethlo.keyvalue.BatchWriteWrapper;
import com.ethlo.keyvalue.IterableKeyValueDb;
import com.ethlo.keyvalue.MutatingKeyValueDb;
import com.ethlo.keyvalue.SeekableIterator;
import com.ethlo.keyvalue.cas.BatchCasKeyValueDb;
import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.cas.CasKeyValueDb;
import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;
import com.google.common.collect.AbstractIterator;

/**
 * Works by using standard SQL for handling data operations instead of the MySql-MemCached interface
 *
 * @author Morten Haraldsen
 */
public class MysqlClientImpl implements
        MutatingKeyValueDb<ByteArrayKey, byte[]>,
        IterableKeyValueDb<ByteArrayKey, byte[]>,
        BatchKeyValueDb<ByteArrayKey, byte[]>,
        BatchCasKeyValueDb<ByteArrayKey, byte[], Long>,
        CasKeyValueDb<ByteArrayKey, byte[], Long>
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlClientImpl.class);

    private JdbcTemplate tpl;

    private final KeyEncoder keyEncoder;
    private final DataCompressor dataCompressor;

    private final String getSql;
    private final String getCasSql;
    private final String getCasSqlPrefix;
    private final String getCasSqlFirst;
    private final String insertSql;
    private final String replaceSql;
    private final String replaceCasSql;
    private final String deleteSql;
    private final String clearSql;
    private final RowMapper<byte[]> rowMapper;
    private final RowMapper<CasHolder<ByteArrayKey, byte[], Long>> casRowMapper;

    public MysqlClientImpl(String tableName, DataSource dataSource, KeyEncoder keyEncoder, DataCompressor dataCompressor)
    {
        this.keyEncoder = keyEncoder;
        this.dataCompressor = dataCompressor;

        Assert.hasLength(tableName, "tableName cannot be null");
        Assert.notNull(dataSource, "dataSource cannot be null");
        this.tpl = new JdbcTemplate(dataSource);

        this.getSql = "SELECT mvalue FROM " + tableName + " WHERE mkey = ?";
        this.getCasSql = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey = ?";
        this.getCasSqlFirst = "SELECT mkey, mvalue, cas_column FROM " + tableName + " ORDER BY mkey";
        this.getCasSqlPrefix = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey LIKE ?";
        this.replaceSql = "REPLACE INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, COALESCE(0, cas_column + 1))";
        this.insertSql = "INSERT INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, ?)";
        this.replaceCasSql = "UPDATE " + tableName + " SET mvalue = ?, cas_column = cas_column + 1 WHERE mkey = ? AND cas_column = ?";
        this.deleteSql = "DELETE FROM " + tableName + " WHERE mkey = ?";
        this.clearSql = "DELETE FROM " + tableName;

        this.rowMapper = (rs, rowNum) ->
        {
            final byte[] data = rs.getBytes(1);
            return dataCompressor.decompress(data);
        };

        this.casRowMapper = (rs, rowNum) ->
        {
            final ByteArrayKey key = new ByteArrayKey(keyEncoder.fromString(rs.getString(1)));
            final byte[] data = rs.getBytes(2);
            final byte[] value = dataCompressor.decompress(data);
            final long cas = rs.getLong(3);
            return new CasHolder<ByteArrayKey, byte[], Long>(cas, key, value);
        };
    }

    @Override
    public byte[] get(final ByteArrayKey key)
    {
        final PreparedStatementCreator getPsc = connection ->
        {
            final PreparedStatement ps = connection.prepareStatement(getSql);
            final String strKey = keyEncoder.toString(key.getByteArray());
            ps.setString(1, strKey);
            return ps;
        };
        return DataAccessUtils.singleResult(tpl.query(getPsc, rowMapper));
    }

    @Override
    public void put(final ByteArrayKey key, final byte[] value)
    {
        putAll(Collections.singletonMap(key, value));
    }

    @Override
    public void putAll(final Map<ByteArrayKey, byte[]> values)
    {
        final List<ByteArrayKey> keys = new ArrayList<>(values.keySet());
        int[] updateCounts = tpl.batchUpdate(replaceSql, new BatchPreparedStatementSetter()
        {
            public void setValues(PreparedStatement ps, int i) throws SQLException
            {
                final ByteArrayKey key = keys.get(i);
                final byte[] value = values.get(key);
                ps.setString(1, keyEncoder.toString(key.getByteArray()));
                ps.setBytes(2, dataCompressor.compress(value));
            }

            public int getBatchSize()
            {
                return values.size();
            }
        });

        logger.debug("Updated {} entries", updateCounts.length);
    }

    @Override
    public void delete(ByteArrayKey key)
    {
        tpl.update(deleteSql, keyEncoder.toString(key.getByteArray()));
    }

    @Override
    public void clear()
    {
        final PreparedStatementCreator creator = connection -> connection.prepareStatement(clearSql);
        tpl.update(creator);
    }

    @Override
    public void close()
    {

    }

    @Override
    public CasHolder<ByteArrayKey, byte[], Long> getCas(final ByteArrayKey key)
    {
        final PreparedStatementCreator getCasPsc = con ->
        {
            final PreparedStatement ps = con.prepareStatement(getCasSql);
            final String strKey = keyEncoder.toString(key.getByteArray());
            ps.setString(1, strKey);
            return ps;
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
        final PreparedStatementCreator creator = connection -> {
            final PreparedStatement ps = connection.prepareStatement(insertSql);
            final String strKey = keyEncoder.toString(cas.getKey().getByteArray());
            ps.setString(1, strKey);
            ps.setBytes(2, dataCompressor.compress(cas.getValue()));
            ps.setLong(3, 0L);
            return ps;
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
        final PreparedStatementCreator creator = con ->
        {
            final String strKey = keyEncoder.toString(cas.getKey().getByteArray());
            final long casValue = cas.getCasValue();
            final PreparedStatement ps = con.prepareStatement(replaceCasSql);
            ps.setBytes(1, dataCompressor.compress(cas.getValue()));
            ps.setString(2, strKey);
            ps.setLong(3, casValue);
            return ps;
        };
        final int rowsChanged = tpl.update(creator);
        if (rowsChanged == 0)
        {
            throw new OptimisticLockingFailureException("Cannot update data for key " + cas.getKey() + " due to concurrent modification. Details: Attempted CAS value=" + cas.getCasValue());
        }

        //return new CasHolder<>(cas.getCasValue(), cas.getKey(), cas.getValue());
    }

    @Override
    public void putAll(final List<CasHolder<ByteArrayKey, byte[], Long>> casList)
    {
        // Hard to solve in any more efficient way as we need to compare the previous version with the new one
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
            this.putCas(new CasHolder<>(null, key, result));
        }
    }

    @Override
    public SeekableIterator<ByteArrayKey, byte[]> iterator()
    {
        return new JdbcSeekableIterator();
    }

    @Override
    public void putAll(final BatchWriteWrapper<ByteArrayKey, byte[]> batch)
    {
        this.putAll(batch.data());
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
                if (!rs.isAfterLast())
                {
                    final CasHolder<ByteArrayKey, byte[], Long> res = casRowMapper.mapRow(rs, rs.getRow());
                    rs.next();
                    return new AbstractMap.SimpleEntry<>(res.getKey(), res.getValue());
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
                    rs = null;
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
                    ps = null;
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
                    connection = null;
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
                    return new AbstractMap.SimpleEntry<>(res.getKey(), res.getValue());
                }
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
            throw new EmptyResultDataAccessException("No previous result", 1);
        }

        @Override
        public boolean seekTo(ByteArrayKey key)
        {
            final PreparedStatementCreator getCasPsc = con ->
            {
                final PreparedStatement ps = con.prepareStatement(getCasSqlPrefix, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                final String strKey = keyEncoder.toString(key.getByteArray());
                ps.setString(1, strKey + "%");
                return ps;
            };

            try
            {
                close();
                this.connection = tpl.getDataSource().getConnection();
                this.ps = getCasPsc.createPreparedStatement(connection);
                this.rs = ps.executeQuery();
                return this.rs.first();
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }

        @Override
        public boolean seekToFirst()
        {
            final PreparedStatementCreator getCasPsc = connection -> connection.prepareStatement(getCasSqlFirst, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            try
            {
                close();
                this.connection = tpl.getDataSource().getConnection();
                this.ps = getCasPsc.createPreparedStatement(connection);
                this.rs = ps.executeQuery();
                return rs.first();
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }

        @Override
        public boolean seekToLast()
        {
            try
            {
                return rs.last();
            }
            catch (SQLException exc)
            {
                throw new DataAccessResourceFailureException(exc.getMessage(), exc);
            }
        }
    }
}
