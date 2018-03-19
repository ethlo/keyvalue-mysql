package com.ethlo.mycached;

/*-
 * #%L
 * Key/value MySQL implementation
 * %%
 * Copyright (C) 2015 - 2018 Morten Haraldsen (ethlo)
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

import org.apache.commons.io.IOUtils;
import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.springframework.dao.DataAccessResourceFailureException;

/**
 * 
 * @author Morten Haraldsen
 */
public class CompressionUtil
{
	public static byte[] uncompress(byte[] compressed)
	{
		if (compressed == null)
		{
			return null;
		}
		
		final ByteArrayInputStream bin = new ByteArrayInputStream(compressed);
		try
		{
			final SnappyInputStream in = new SnappyInputStream(bin);
			return IOUtils.toByteArray(in);
		}
		catch (IOException exc)
		{
			throw new DataAccessResourceFailureException(exc.getMessage(), exc);
		}
	}

	public static byte[] compress(byte[] data)
	{
		try
		{
			final ByteArrayOutputStream bout = new ByteArrayOutputStream();
			final SnappyOutputStream out = new SnappyOutputStream(bout);
			out.write(data);
			out.close();
			return bout.toByteArray();
		}
		catch (IOException exc)
		{
			throw new DataAccessResourceFailureException(exc.getMessage(), exc);
		}
	}
}
