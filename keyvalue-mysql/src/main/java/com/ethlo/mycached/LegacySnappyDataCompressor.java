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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.springframework.dao.DataAccessResourceFailureException;

import com.ethlo.keyvalue.compression.DataCompressor;
import com.google.common.io.ByteStreams;

@SuppressWarnings({"deprecation", "UnstableApiUsage"})
public class LegacySnappyDataCompressor implements DataCompressor
{
    @Override
    public byte[] compress(byte[] uncompressed)
    {
        try
        {
            final ByteArrayOutputStream bout = new ByteArrayOutputStream(uncompressed.length);
            final org.iq80.snappy.SnappyOutputStream out = new org.iq80.snappy.SnappyOutputStream(bout);
            out.write(uncompressed);
            out.close();
            return bout.toByteArray();
        }
        catch (IOException exc)
        {
            throw new DataAccessResourceFailureException(exc.getMessage(), exc);
        }
    }

    @Override
    public byte[] decompress(byte[] compressed)
    {
        if (compressed == null)
        {
            return null;
        }

        final ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
        try
        {
            final org.iq80.snappy.SnappyInputStream in = new org.iq80.snappy.SnappyInputStream(bin);
            return ByteStreams.toByteArray(in);
        }
        catch (IOException exc)
        {
            throw new DataAccessResourceFailureException(exc.getMessage(), exc);
        }
    }
}
