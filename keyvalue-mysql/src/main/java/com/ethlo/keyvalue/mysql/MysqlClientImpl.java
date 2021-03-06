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


import java.sql.*;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.*;
import org.springframework.util.Assert;

import com.ethlo.keyvalue.BatchWriteWrapper;
import com.ethlo.keyvalue.SeekableIterator;
import com.ethlo.keyvalue.cas.CasHolder;
import com.ethlo.keyvalue.compression.DataCompressor;
import com.ethlo.keyvalue.keys.ByteArrayKey;
import com.ethlo.keyvalue.keys.encoders.KeyEncoder;
import com.google.common.collect.AbstractIterator;

/**
 * Works by using standard SQL for handling data operations instead of the MySql-MemCached interface
 *
 * @author Morten Haraldsen
 */
public class MysqlClientImpl implements MysqlClient
{
    private static final Logger logger = LoggerFactory.getLogger(MysqlClientImpl.class);

    private final JdbcTemplate tpl;

    private final KeyEncoder keyEncoder;
    private final DataCompressor dataCompressor;

    private final String getCasSqlPrefix;
    private final String getCasSqlFirst;
    private final String deleteSql;
    private final String insertOnDuplicateUpdateSql;
    private final String replaceInto;

    private final RowMapper<byte[]> rowMapper;
    private final RowMapper<CasHolder<ByteArrayKey, byte[], Long>> casRowMapper;

    private final boolean useReplaceInto;

    private final PreparedStatementCreatorFactory getDataCasPscFactory;
    private final PreparedStatementCreatorFactory clearPscFactory;
    private final PreparedStatementCreatorFactory insertCasPscFactory;
    private final PreparedStatementCreatorFactory updateCasPscFactory;
    private final PreparedStatementCreatorFactory getDataPscFactory;

    public MysqlClientImpl(String tableName, DataSource dataSource, KeyEncoder keyEncoder, DataCompressor dataCompressor)
    {
        this(tableName, dataSource, keyEncoder, dataCompressor, false);
    }

    public MysqlClientImpl(String tableName, DataSource dataSource, KeyEncoder keyEncoder, DataCompressor dataCompressor, boolean useReplaceInto)
    {
        this.keyEncoder = keyEncoder;
        this.dataCompressor = dataCompressor;
        this.useReplaceInto = useReplaceInto;

        Assert.hasLength(tableName, "tableName cannot be null");
        Assert.notNull(dataSource, "dataSource cannot be null");
        this.tpl = new JdbcTemplate(dataSource);

        String getSql = "SELECT mvalue FROM " + tableName + " WHERE mkey = ?";
        String getCasSql = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey = ?";
        this.getCasSqlFirst = "SELECT mkey, mvalue, cas_column FROM " + tableName + " ORDER BY mkey";
        this.getCasSqlPrefix = "SELECT mkey, mvalue, cas_column FROM " + tableName + " WHERE mkey LIKE ?";
        String insertCasSql = "INSERT INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, ?)";
        String updateCasSql = "UPDATE " + tableName + " SET mvalue = ?, cas_column = cas_column + 1 WHERE mkey = ? AND cas_column = ?";
        this.insertOnDuplicateUpdateSql = "INSERT INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE mvalue=?, cas_column=cas_column+1";
        this.replaceInto = "REPLACE INTO " + tableName + " (mkey, mvalue, cas_column) VALUES(?, ?, COALESCE(0, cas_column + 1))";
        this.deleteSql = "DELETE FROM " + tableName + " WHERE mkey = ?";
        String clearSql = "DELETE FROM " + tableName;

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
            return new CasHolder<>(cas, key, value);
        };

        getDataPscFactory = new PreparedStatementCreatorFactory(getSql, Types.VARCHAR);
        getDataCasPscFactory = new PreparedStatementCreatorFactory(getCasSql, Types.VARCHAR);
        clearPscFactory = new PreparedStatementCreatorFactory(clearSql);
        insertCasPscFactory = new PreparedStatementCreatorFactory(insertCasSql, Types.VARCHAR, Types.BINARY, Types.INTEGER);
        updateCasPscFactory = new PreparedStatementCreatorFactory(updateCasSql, Types.BINARY, Types.VARCHAR, Types.INTEGER);
    }

    @Override
    public byte[] get(final ByteArrayKey key)
    {
        return getData(key, getDataPscFactory, rowMapper);
    }

    private <T> T getData(final ByteArrayKey key, final PreparedStatementCreatorFactory factory, final RowMapper<T> rowMapper)
    {
        final String strKey = keyEncoder.toString(key.getByteArray());
        final PreparedStatementCreator getPsc = factory.newPreparedStatementCreator(Collections.singletonList(strKey));
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
        if (!useReplaceInto)
        {
            insertOnDuplicateKeyUpdate(values);
        }
        else
        {
            replaceInto(values);
        }
    }

    private void insertOnDuplicateKeyUpdate(final Map<ByteArrayKey, byte[]> values)
    {
        final List<ByteArrayKey> keys = new ArrayList<>(values.keySet());
        int[] updateCounts = tpl.batchUpdate(insertOnDuplicateUpdateSql, new BatchPreparedStatementSetter()
        {
            public void setValues(@Nonnull PreparedStatement ps, int i) throws SQLException
            {
                final ByteArrayKey key = keys.get(i);
                final byte[] value = values.get(key);
                final byte[] compressedValue = dataCompressor.compress(value);
                ps.setString(1, keyEncoder.toString(key.getByteArray()));
                ps.setBytes(2, compressedValue);
                ps.setLong(3, 0L);
                ps.setBytes(4, compressedValue);
            }

            public int getBatchSize()
            {
                return values.size();
            }
        });

        logger.debug("Updated {} entries", updateCounts.length);
    }

    private void replaceInto(final Map<ByteArrayKey, byte[]> values)
    {
        final List<ByteArrayKey> keys = new ArrayList<>(values.keySet());
        int[] updateCounts = tpl.batchUpdate(replaceInto, new BatchPreparedStatementSetter()
        {
            public void setValues(@Nonnull PreparedStatement ps, int i) throws SQLException
            {
                final ByteArrayKey key = keys.get(i);
                final byte[] value = values.get(key);
                final byte[] compressedValue = dataCompressor.compress(value);
                ps.setString(1, keyEncoder.toString(key.getByteArray()));
                ps.setBytes(2, compressedValue);
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
        tpl.update(clearPscFactory.newPreparedStatementCreator(new Object[0]));
    }

    @Override
    public void close()
    {

    }

    @Override
    public CasHolder<ByteArrayKey, byte[], Long> getCas(final ByteArrayKey key)
    {
        return getData(key, getDataCasPscFactory, casRowMapper);
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
        final String strKey = keyEncoder.toString(cas.getKey().getByteArray());
        final byte[] value = dataCompressor.compress(cas.getValue());
        final PreparedStatementCreator psc = insertCasPscFactory.newPreparedStatementCreator(Arrays.asList(strKey, value, 0L));

        try
        {
            tpl.update(psc);
        }
        catch (DuplicateKeyException exc)
        {
            throw new OptimisticLockingFailureException("Cannot update " + cas.getKey(), exc);
        }
    }

    private void updateWithNonNullCasValue(final CasHolder<ByteArrayKey, byte[], Long> cas)
    {
        final String strKey = keyEncoder.toString(cas.getKey().getByteArray());
        final long casValue = cas.getCasValue();
        final byte[] value = dataCompressor.compress(cas.getValue());

        final int rowsChanged = tpl.update(updateCasPscFactory.newPreparedStatementCreator(Arrays.asList(value, strKey, casValue)));
        if (rowsChanged == 0)
        {
            throw new OptimisticLockingFailureException("Cannot update data for key " + cas.getKey() + " due to concurrent modification. Details: Attempted CAS value=" + cas.getCasValue());
        }
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
                    return new AbstractMap.SimpleEntry<>(Objects.requireNonNull(res).getKey(), res.getValue());
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
                    return new AbstractMap.SimpleEntry<>(Objects.requireNonNull(res).getKey(), res.getValue());
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
                this.connection = Objects.requireNonNull(tpl.getDataSource()).getConnection();
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
                this.connection = Objects.requireNonNull(tpl.getDataSource()).getConnection();
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
